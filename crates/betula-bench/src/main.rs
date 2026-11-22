//! Benchmarks betula against SlateDB on an S3-compatible store (e.g. RustFS).

mod engine;
mod metered;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use clap::Parser;
use futures::TryStreamExt;
use hdrhistogram::Histogram;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};

use engine::{BetulaParams, Engine, EngineKind};
use metered::{Counters, Metered};

#[derive(Parser, Debug)]
#[command(about = "Benchmark betula vs SlateDB on an S3-compatible store")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9100")]
    endpoint: String,
    #[arg(long, default_value = "bench")]
    bucket: String,
    #[arg(long, env = "BENCH_ACCESS_KEY", default_value = "betula")]
    access_key: String,
    #[arg(long, env = "BENCH_SECRET_KEY", default_value = "betula-secret")]
    secret_key: String,
    #[arg(long, value_delimiter = ',', default_values = ["betula", "slatedb"])]
    engines: Vec<EngineKind>,
    /// Keys loaded before the read phases.
    #[arg(long, default_value_t = 200_000)]
    keys: u64,
    #[arg(long, default_value_t = 256)]
    value_size: usize,
    /// Concurrent clients per phase.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value_t = 50_000)]
    reads: u64,
    #[arg(long, default_value_t = 2_000)]
    scans: u64,
    #[arg(long, default_value_t = 100)]
    scan_len: usize,
    #[arg(long, default_value_t = 50_000)]
    updates: u64,
    /// Keys per write batch in the load and update phases. Their ops/s count keys;
    /// latencies are per batch.
    #[arg(long, default_value_t = 32)]
    batch: u64,
    /// Delay added to every object store request, to approximate S3 latency.
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    #[arg(long, default_value_t = 64)]
    leaf_kib: usize,
    #[arg(long, default_value_t = 1024)]
    internal_kib: usize,
    #[arg(long, default_value_t = 32)]
    fanout: usize,
    /// betula: load buffered runs in the background on open instead of
    /// before returning.
    #[arg(long)]
    warm_background: bool,
    /// betula: buffered runs per child before it is flushed.
    #[arg(long, default_value_t = 16)]
    max_runs: usize,
    #[arg(long, default_value_t = 16)]
    memtable_mib: usize,
    /// betula cache split. Defaults total 640 MiB, the same as SlateDB's
    /// default block + metadata caches.
    #[arg(long, default_value_t = 256)]
    inner_cache_mib: u64,
    #[arg(long, default_value_t = 384)]
    leaf_cache_mib: u64,
    /// Concurrent betula writers on the database (clients are spread across
    /// them). SlateDB allows one.
    #[arg(long, default_value_t = 1)]
    writers: usize,
    /// How often betula writers poll for each other's entries.
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,
    /// Cross-writer visibility samples (betula, 2+ writers).
    #[arg(long, default_value_t = 100)]
    visibility_samples: u64,
    /// betula cleans segments whose live fraction is below this.
    #[arg(long, default_value_t = 0.25)]
    clean_below: f64,
}

// ---------------------------------------------------------------------------
// Workload

/// Deterministic, cheap pseudo-randomness so runs are comparable.
fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

struct Workload {
    keys: u64,
    value_size: usize,
    pool: Bytes,
    /// Coprime with `keys`, so `i * stride % keys` permutes the key space.
    stride: u64,
}

impl Workload {
    fn new(keys: u64, value_size: usize) -> Self {
        let pool: Vec<u8> = (0..(4 << 20) / 8)
            .flat_map(|i| splitmix(i).to_le_bytes())
            .collect();
        let stride = (0x9E37_79B9u64..)
            .find(|s| gcd(*s, keys) == 1)
            .expect("a coprime exists");
        Self {
            keys,
            value_size,
            pool: Bytes::from(pool),
            stride,
        }
    }

    fn key(&self, id: u64) -> Bytes {
        Bytes::from(format!("key{id:012}"))
    }

    /// Value for `id` at a given version; versions make updates checkable.
    fn value(&self, id: u64, version: u64) -> Bytes {
        let span = (self.pool.len() - self.value_size) as u64;
        let off = (splitmix(id ^ (version << 48)) % span) as usize;
        self.pool.slice(off..off + self.value_size)
    }

    fn load_order(&self, i: u64) -> u64 {
        (i % self.keys) * (self.stride % self.keys) % self.keys
    }

    fn random_id(&self, seed: u64, i: u64) -> u64 {
        splitmix(seed.wrapping_mul(1_000_003) ^ i) % self.keys
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

// ---------------------------------------------------------------------------
// Measurement

struct Phase {
    name: &'static str,
    ops: u64,
    elapsed: Duration,
    hist: Histogram<u64>,
    io: metered::Snapshot,
}

/// Runs `ops` operations across `concurrency` tasks, recording per-op latency.
async fn run_ops<F, Fut>(ops: u64, concurrency: usize, op: F) -> Result<(Duration, Histogram<u64>)>
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    let op = Arc::new(op);
    let next = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let tasks: Vec<_> = (0..concurrency)
        .map(|_| {
            let (op, next) = (op.clone(), next.clone());
            tokio::spawn(async move {
                let mut hist = Histogram::<u64>::new(3)?;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= ops {
                        return Ok::<_, anyhow::Error>(hist);
                    }
                    let t = Instant::now();
                    op(i).await?;
                    hist.record(t.elapsed().as_micros() as u64)?;
                }
            })
        })
        .collect();
    let hists = futures::future::try_join_all(tasks).await?;
    let elapsed = start.elapsed();
    let hist = hists
        .into_iter()
        .try_fold(Histogram::<u64>::new(3)?, |mut acc, h| {
            acc.add(h?)?;
            Ok::<_, anyhow::Error>(acc)
        })?;
    Ok((elapsed, hist))
}

/// Writes `total` keys (load order from `offset`) in batches of `batch`.
async fn write_phase(
    db: Arc<Engine>,
    wl: Arc<Workload>,
    total: u64,
    batch: u64,
    concurrency: usize,
    version: u64,
    offset: u64,
) -> Result<(Duration, Histogram<u64>)> {
    run_ops(total.div_ceil(batch), concurrency, move |b| {
        let (db, wl) = (db.clone(), wl.clone());
        async move {
            let pairs = (b * batch..((b + 1) * batch).min(total))
                .map(|i| wl.load_order(i + offset))
                .map(|id| (wl.key(id), wl.value(id, version)))
                .collect();
            db.write(b, pairs).await
        }
    })
    .await
}

async fn timed<T>(f: impl Future<Output = Result<T>>) -> Result<(Duration, Histogram<u64>, T)> {
    let t = Instant::now();
    let out = f.await?;
    let elapsed = t.elapsed();
    let mut hist = Histogram::<u64>::new(3)?;
    hist.record(elapsed.as_micros() as u64)?;
    Ok((elapsed, hist, out))
}

fn print_header() {
    println!(
        "| engine | phase | ops | ops/s | p50 ms | p99 ms | p99.9 ms | GET | GET MiB | PUT | PUT MiB | LIST | DEL |"
    );
    println!("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
}

fn print_phase(engine: &str, p: &Phase) {
    let ms = |q: f64| p.hist.value_at_quantile(q) as f64 / 1000.0;
    let mib = |b: u64| b as f64 / (1 << 20) as f64;
    println!(
        "| {engine} | {} | {} | {:.0} | {:.2} | {:.2} | {:.2} | {} | {:.1} | {} | {:.1} | {} | {} |",
        p.name,
        p.ops,
        p.ops as f64 / p.elapsed.as_secs_f64(),
        ms(0.5),
        ms(0.99),
        ms(0.999),
        p.io.gets,
        mib(p.io.get_bytes),
        p.io.puts,
        mib(p.io.put_bytes),
        p.io.lists,
        p.io.deletes,
    );
}

// ---------------------------------------------------------------------------
// Main

async fn preflight(os: &Arc<dyn ObjectStore>) -> Result<()> {
    let path = Path::from(format!("preflight/{}", unique()));
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    os.put_opts(&path, "a".into(), create())
        .await
        .context("store unreachable or bucket missing")?;
    let second = os.put_opts(&path, "b".into(), create()).await;
    os.delete(&path).await?;
    match second {
        Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        other => bail!("store lacks put-if-absent (If-None-Match): {other:?}"),
    }
}

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

async fn footprint(os: &Arc<dyn ObjectStore>, path: &str) -> Result<(usize, u64)> {
    let metas: Vec<_> = os.list(Some(&Path::from(path))).try_collect().await?;
    Ok((metas.len(), metas.iter().map(|m| m.size).sum()))
}

async fn bench_engine(
    args: &Args,
    kind: EngineKind,
    raw: Arc<dyn ObjectStore>,
    wl: Arc<Workload>,
) -> Result<Vec<Phase>> {
    let metered = Arc::new(Metered::new(
        raw.clone(),
        Duration::from_millis(args.delay_ms),
    ));
    let counters: Arc<Counters> = metered.counters.clone();
    let os: Arc<dyn ObjectStore> = metered;
    let path = format!("runs/{}-{}", kind.name(), unique());
    let params = BetulaParams {
        leaf_bytes: args.leaf_kib << 10,
        internal_bytes: args.internal_kib << 10,
        fanout: args.fanout,
        memtable_bytes: args.memtable_mib << 20,
        inner_cache_bytes: args.inner_cache_mib << 20,
        leaf_cache_bytes: args.leaf_cache_mib << 20,
        clean_below_live: args.clean_below,
        poll: Duration::from_millis(args.poll_ms),
        max_runs: args.max_runs,
        warm: if args.warm_background {
            betula::Warm::Background
        } else {
            betula::Warm::Blocking
        },
    };
    let writers = match kind {
        EngineKind::Betula => args.writers,
        EngineKind::Slatedb => 1,
    };
    let c = args.concurrency;
    let mut phases = Vec::new();
    let mut io_mark = counters.snapshot();
    let mut record = |name, ops, elapsed, hist, phases: &mut Vec<Phase>| {
        let now = counters.snapshot();
        phases.push(Phase {
            name,
            ops,
            elapsed,
            hist,
            io: now - io_mark,
        });
        io_mark = now;
    };

    let db = Arc::new(Engine::open(kind, os.clone(), &path, &params, writers).await?);

    let (elapsed, hist) = write_phase(db.clone(), wl.clone(), wl.keys, args.batch, c, 0, 0).await?;
    record("load", wl.keys, elapsed, hist, &mut phases);

    let (elapsed, hist, ()) = timed(db.settle()).await?;
    record("settle", 1, elapsed, hist, &mut phases);
    eprintln!(
        "[{}] loaded {} keys {}",
        kind.name(),
        wl.keys,
        db.describe()
    );

    if writers >= 2 && args.visibility_samples > 0 {
        let started = Instant::now();
        let mut hist = Histogram::<u64>::new(3)?;
        for i in 0..args.visibility_samples {
            let key = Bytes::from(format!("vis{i:08}"));
            if let Some(lag) = db.visibility_lag(key, wl.value(i, 9)).await? {
                hist.record(lag.as_micros() as u64)?;
            }
        }
        record(
            "visibility",
            args.visibility_samples,
            started.elapsed(),
            hist,
            &mut phases,
        );
    }

    let read_phase = |seed: u64, db: Arc<Engine>, version: u64| {
        let wl = wl.clone();
        run_ops(args.reads, c, move |i| {
            let (db, wl) = (db.clone(), wl.clone());
            async move {
                let id = wl.random_id(seed, i);
                let got = db.get(&wl.key(id)).await?;
                ensure!(
                    got == Some(wl.value(id, version)),
                    "wrong value for key {id}"
                );
                Ok(())
            }
        })
    };
    let (elapsed, hist) = read_phase(1, db.clone(), 0).await?;
    record("read-warm", args.reads, elapsed, hist, &mut phases);

    let (elapsed, hist) = {
        let (db, wl, len) = (db.clone(), wl.clone(), args.scan_len);
        run_ops(args.scans, c, move |i| {
            let (db, wl) = (db.clone(), wl.clone());
            async move {
                let start = wl.random_id(2, i).min(wl.keys - len as u64);
                let n = db.scan(wl.key(start), len).await?;
                ensure!(n == len, "scan returned {n}, expected {len}");
                Ok(())
            }
        })
        .await?
    };
    record("scan", args.scans, elapsed, hist, &mut phases);

    let db = Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("engine still shared"))?;
    let (elapsed, hist, ()) = timed(db.close()).await?;
    record("close", 1, elapsed, hist, &mut phases);
    let (elapsed, hist, db) =
        timed(Engine::open(kind, os.clone(), &path, &params, writers)).await?;
    record("reopen", 1, elapsed, hist, &mut phases);
    let db = Arc::new(db);

    let (elapsed, hist) = read_phase(3, db.clone(), 0).await?;
    record("read-cold", args.reads, elapsed, hist, &mut phases);

    // Offset into the load order: different keys than the first loaded,
    // distinct so each is checkable as version 1.
    let updates = args.updates.min(wl.keys);
    let (elapsed, hist) = write_phase(
        db.clone(),
        wl.clone(),
        updates,
        args.batch,
        c,
        1,
        wl.keys / 3,
    )
    .await?;
    record("update", updates, elapsed, hist, &mut phases);

    let (elapsed, hist, ()) = timed(db.settle()).await?;
    record("settle", 1, elapsed, hist, &mut phases);
    // Spot-check that updates landed.
    let probe = wl.load_order(wl.keys / 3);
    ensure!(
        db.get(&wl.key(probe)).await? == Some(wl.value(probe, 1)),
        "update lost"
    );

    let db = Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("engine still shared"))?;
    db.close().await?;
    let (objects, bytes) = footprint(&raw, &path).await?;
    eprintln!(
        "[{}] footprint: {objects} objects, {:.1} MiB (logical data {:.1} MiB)",
        kind.name(),
        bytes as f64 / (1 << 20) as f64,
        (wl.keys as usize * (wl.value_size + 15)) as f64 / (1 << 20) as f64
    );
    Ok(phases)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let raw: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(&args.endpoint)
            .with_bucket_name(&args.bucket)
            .with_access_key_id(&args.access_key)
            .with_secret_access_key(&args.secret_key)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    preflight(&raw).await?;
    let wl = Arc::new(Workload::new(args.keys, args.value_size));
    eprintln!("{args:?}");

    let mut results = Vec::new();
    for &kind in &args.engines {
        let phases = bench_engine(&args, kind, raw.clone(), wl.clone()).await?;
        results.push((kind, phases));
    }
    print_header();
    results
        .iter()
        .flat_map(|(k, ps)| ps.iter().map(move |p| (k, p)))
        .for_each(|(k, p)| print_phase(k.name(), p));
    Ok(())
}
