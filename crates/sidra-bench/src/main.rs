//! Benchmarks sidra against SlateDB on an S3-compatible store (e.g. RustFS).

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

use engine::{Engine, EngineKind, EngineParams};
use metered::{Counters, Metered};

#[derive(Parser, Debug)]
#[command(about = "Benchmark sidra vs SlateDB on an S3-compatible store")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9100")]
    endpoint: String,
    #[arg(long, default_value = "bench")]
    bucket: String,
    #[arg(long, env = "BENCH_ACCESS_KEY", default_value = "sidra")]
    access_key: String,
    #[arg(long, env = "BENCH_SECRET_KEY", default_value = "sidra-secret")]
    secret_key: String,
    #[arg(long, value_delimiter = ',', default_values = ["sidra", "slatedb"])]
    engines: Vec<EngineKind>,
    /// Keys loaded before the read phases.
    #[arg(long, default_value_t = 200_000)]
    keys: u64,
    #[arg(long, default_value_t = 256)]
    value_size: usize,
    /// `random` (incompressible) or `text` (JSON-like, compressible).
    #[arg(long, value_enum, default_value_t = ValueKind::Random)]
    values: ValueKind,
    /// Compression for both engines.
    #[arg(long, value_enum, default_value_t = CompressionArg::None)]
    compression: CompressionArg,
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
    /// sidra: load buffered runs in the background on open instead of
    /// before returning.
    #[arg(long)]
    warm_background: bool,
    /// SlateDB: bytes per block fetch in scans. SlateDB's own default reads
    /// one block per request. No setting wins every scan: 4 KiB / 1 task is
    /// best for warm 100-key scans, 256 KiB / 8 for cold 10,000-key ones;
    /// 64 KiB / 4 wins the rest.
    #[arg(long, default_value_t = 64)]
    slatedb_read_ahead_kib: usize,
    /// SlateDB: concurrent block fetches per scan. SlateDB's own default is 1.
    #[arg(long, default_value_t = 4)]
    slatedb_fetch_tasks: usize,
    /// sidra: buffered runs per child before it is flushed.
    #[arg(long, default_value_t = 16)]
    max_runs: usize,
    #[arg(long, default_value_t = 16)]
    memtable_mib: usize,
    /// sidra cache split. Defaults total 640 MiB, the same as SlateDB's
    /// default block + metadata caches.
    #[arg(long, default_value_t = 256)]
    inner_cache_mib: u64,
    #[arg(long, default_value_t = 384)]
    leaf_cache_mib: u64,
    /// Concurrent sidra writers on the database (clients are spread across
    /// them). SlateDB allows one.
    #[arg(long, default_value_t = 1)]
    writers: usize,
    /// How often sidra writers poll for each other's entries.
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,
    /// Cross-writer visibility samples (sidra, 2+ writers).
    #[arg(long, default_value_t = 100)]
    visibility_samples: u64,
    /// Seconds of mixed reads and writes after the other phases (0: skip).
    /// Shows how each engine behaves while background work (compaction,
    /// checkpoints, cleaning) runs alongside traffic.
    #[arg(long, default_value_t = 0)]
    mixed_secs: u64,
    /// After everything else, delete this percentage of keys, spread evenly
    /// over the key space, then measure scans over what's left (warm, then
    /// cold after a reopen). Shows the cost of sparse leaves.
    #[arg(long, default_value_t = 0)]
    delete_pct: u64,
    /// Keep each run's database in the bucket (deleted after the run by
    /// default: runs add up to hundreds of GB).
    #[arg(long)]
    keep_data: bool,
    /// Percentage of the mixed phase's clients that only read; the rest only
    /// write batches (separate clients, so slow durable writes don't throttle
    /// reads).
    #[arg(long, default_value_t = 50)]
    mixed_read_pct: u64,
    /// Width of the windows in the mixed phase's read-latency timeline.
    #[arg(long, default_value_t = 30)]
    mixed_window_secs: u64,
    /// sidra cleans segments whose live fraction is below this.
    #[arg(long, default_value_t = 0.25)]
    clean_below: f64,
    /// sidra: disable garbage collection (for experiments).
    #[arg(long)]
    no_gc: bool,
    /// sidra: GC delete rate cap.
    #[arg(long, default_value_t = 100)]
    gc_deletes_per_sec: u32,
    /// sidra cleans inner segments (runs) below this live fraction.
    #[arg(long, default_value_t = 0.1)]
    clean_inner_below: f64,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum ValueKind {
    Random,
    Text,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum CompressionArg {
    None,
    Lz4,
}

struct Workload {
    keys: u64,
    value_size: usize,
    pool: Bytes,
    /// Coprime with `keys`, so `i * stride % keys` permutes the key space.
    stride: u64,
}

impl Workload {
    fn new(keys: u64, value_size: usize, values: ValueKind) -> Self {
        let pool: Vec<u8> = match values {
            ValueKind::Random => (0..(4 << 20) / 8)
                .flat_map(|i| splitmix(i).to_le_bytes())
                .collect(),
            // JSON-like records with varying fields: compresses about as
            // well as typical application data.
            ValueKind::Text => {
                let plans = ["free", "pro", "team", "enterprise"];
                let countries = ["EG", "AE", "SA", "US", "DE", "IN", "BR", "JP"];
                (0..)
                    .map(|i: u64| {
                        let r = splitmix(i);
                        format!(
                            r#"{{"user_id":{},"name":"user{}","email":"user{}@example.com","country":"{}","plan":"{}","score":{},"active":{},"tags":["t{}","t{}"]}}"#,
                            r % 10_000_000,
                            r % 10_000_000,
                            r % 10_000_000,
                            countries[(r >> 8) as usize % countries.len()],
                            plans[(r >> 16) as usize % plans.len()],
                            (r >> 24) % 1000,
                            (r >> 34).is_multiple_of(2),
                            (r >> 40) % 50,
                            (r >> 48) % 50,
                        )
                    })
                    .flat_map(String::into_bytes)
                    .take(4 << 20)
                    .collect()
            }
        };
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

/// Whether key `id` is deleted by the delete phase: `pct` out of every 100
/// consecutive ids, so every leaf loses about the same share.
fn doomed(id: u64, pct: u64) -> bool {
    id % 100 < pct
}

/// Deletes `pct`% of keys, settles, then scans the survivors warm and (after
/// a reopen) cold. Returns the reopened engine.
async fn delete_phases<F, Fut>(
    args: &Args,
    kind: EngineKind,
    db: Arc<Engine>,
    wl: Arc<Workload>,
    pct: u64,
    reopen: F,
    record: &mut impl FnMut(&'static str, u64, Duration, Histogram<u64>),
) -> Result<Arc<Engine>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Engine>>,
{
    let ids: Arc<Vec<u64>> = Arc::new((0..wl.keys).filter(|id| doomed(*id, pct)).collect());
    let (c, batch) = (args.concurrency, args.batch);
    let (elapsed, hist) = {
        let (db, ids) = (db.clone(), ids.clone());
        let wl = wl.clone();
        run_ops((ids.len() as u64).div_ceil(batch), c, move |b| {
            let (db, ids, wl) = (db.clone(), ids.clone(), wl.clone());
            async move {
                let lo = (b * batch) as usize;
                let hi = (lo + batch as usize).min(ids.len());
                db.delete(b, ids[lo..hi].iter().map(|id| wl.key(*id)).collect())
                    .await
            }
        })
        .await?
    };
    record("delete", ids.len() as u64, elapsed, hist);
    let (elapsed, hist, ()) = timed(db.settle()).await?;
    record("settle", 1, elapsed, hist);
    if let Some(shape) = db.shape().await? {
        eprintln!("[{}] after deletes: {shape}", kind.name());
    }

    let survivors = wl.keys - ids.len() as u64;
    ensure!(survivors > 0, "--delete-pct leaves no keys");
    let len = args.scan_len.min(survivors as usize);
    // Scans start where at least `len` survivors follow.
    let span = (len as u64 * 100).div_ceil(100 - pct) + 100;
    let scans = |seed: u64, db: Arc<Engine>| {
        let wl = wl.clone();
        run_ops(args.scans, c, move |i| {
            let (db, wl) = (db.clone(), wl.clone());
            async move {
                let start = wl.random_id(seed, i).min(wl.keys.saturating_sub(span));
                let n = db.scan(wl.key(start), len).await?;
                ensure!(n == len, "scan returned {n}, expected {len}");
                Ok(())
            }
        })
    };
    // Warm both caches first, as before the warm phases of the main run.
    let (elapsed, hist, n) = timed(db.warm_up(wl.key(0), survivors as usize)).await?;
    ensure!(n == survivors as usize, "warm-up scan returned {n} keys");
    record("warmup", 1, elapsed, hist);
    let (elapsed, hist) = scans(4, db.clone()).await?;
    record("scan-sparse", args.scans, elapsed, hist);

    let db = Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("engine still shared"))?;
    db.close().await?;
    let (elapsed, hist, db) = timed(reopen()).await?;
    record("reopen", 1, elapsed, hist);
    let db = Arc::new(db);
    let (elapsed, hist) = scans(5, db.clone()).await?;
    record("scan-sparse-cold", args.scans, elapsed, hist);
    // Spot-check: a deleted key is gone, a survivor isn't.
    ensure!(
        db.get(&wl.key(0)).await?.is_none(),
        "deleted key still present"
    );
    ensure!(
        db.get(&wl.key(pct)).await?.is_some(),
        "surviving key missing"
    );
    Ok(db)
}

/// Mixed reads and write batches for `args.mixed_secs`. Prints a read-latency
/// timeline per window to stderr; returns totals for reads and writes.
async fn mixed_phase(
    args: &Args,
    kind: EngineKind,
    db: Arc<Engine>,
    wl: Arc<Workload>,
    counters: &Counters,
) -> Result<Vec<(&'static str, u64, Duration, Histogram<u64>)>> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.mixed_secs);
    let window = Duration::from_secs(args.mixed_window_secs.max(1));
    let windows = args.mixed_secs.div_ceil(args.mixed_window_secs.max(1)) as usize;
    let next = Arc::new(AtomicU64::new(0));
    let batch = args.batch;
    let readers =
        (args.concurrency as u64 * args.mixed_read_pct / 100).clamp(1, args.concurrency as u64 - 1);
    let tasks: Vec<_> = (0..args.concurrency as u64)
        .map(|task| {
            let reader = task < readers;
            let (db, wl, next) = (db.clone(), wl.clone(), next.clone());
            tokio::spawn(async move {
                let mut reads = Histogram::<u64>::new(3)?;
                let mut writes = Histogram::<u64>::new(3)?;
                // Per window: (reads, writes).
                let mut timeline: Vec<(Histogram<u64>, Histogram<u64>)> = (0..windows)
                    .map(|_| {
                        Ok::<_, hdrhistogram::CreationError>((
                            Histogram::new(3)?,
                            Histogram::new(3)?,
                        ))
                    })
                    .collect::<Result<_, _>>()?;
                while Instant::now() < deadline {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let t = Instant::now();
                    let w = ((t.duration_since(started).as_secs() / window.as_secs()) as usize)
                        .min(windows - 1);
                    if reader {
                        let id = wl.random_id(7, i);
                        // Concurrent updates make any version valid; it must
                        // exist, though.
                        ensure!(db.get(&wl.key(id)).await?.is_some(), "key {id} lost");
                        let us = t.elapsed().as_micros() as u64;
                        reads.record(us)?;
                        timeline[w].0.record(us)?;
                    } else {
                        let pairs = (0..batch)
                            .map(|j| wl.random_id(11, i * batch + j))
                            .map(|id| (wl.key(id), wl.value(id, 2 + i % 7)))
                            .collect();
                        db.write(i, pairs).await?;
                        let us = t.elapsed().as_micros() as u64;
                        writes.record(us)?;
                        timeline[w].1.record(us)?;
                    }
                }
                Ok::<_, anyhow::Error>((reads, writes, timeline))
            })
        })
        .collect();
    let results = futures::future::try_join_all(tasks).await?;
    let elapsed = started.elapsed();
    let mut reads = Histogram::<u64>::new(3)?;
    let mut writes = Histogram::<u64>::new(3)?;
    let mut timeline: Vec<(Histogram<u64>, Histogram<u64>)> = (0..windows)
        .map(|_| Ok::<_, hdrhistogram::CreationError>((Histogram::new(3)?, Histogram::new(3)?)))
        .collect::<Result<_, _>>()?;
    for r in results {
        let (r, w, tl) = r?;
        reads.add(r)?;
        writes.add(w)?;
        for ((ar, aw), (hr, hw)) in timeline.iter_mut().zip(tl) {
            ar.add(hr)?;
            aw.add(hw)?;
        }
    }
    eprintln!(
        "[{}] mixed timeline by {}s window: reads/s, read p99 ms, write p50 / p99 ms",
        kind.name(),
        window.as_secs()
    );
    for (w, (r, wr)) in timeline.iter().enumerate() {
        let ms = |h: &Histogram<u64>, q: f64| h.value_at_quantile(q) as f64 / 1000.0;
        eprintln!(
            "[{}]   {:>4}s {:>8.0} {:>7.2}   {:>7.1} {:>7.1}",
            kind.name(),
            w as u64 * window.as_secs(),
            r.len() as f64 / window.as_secs_f64(),
            ms(r, 0.99),
            ms(wr, 0.5),
            ms(wr, 0.99)
        );
    }
    let io = counters.snapshot();
    eprintln!(
        "[{}] requests after mixed: {} GET, {} PUT",
        kind.name(),
        io.gets,
        io.puts
    );
    let (nr, nw) = (reads.len(), writes.len() * batch);
    Ok(vec![
        ("mixed-read", nr, elapsed, reads),
        ("mixed-write", nw, elapsed, writes),
    ])
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

/// Deletes everything under `path` (a finished run's database), in small
/// bulk requests: one huge request times out on RustFS.
async fn remove_all(os: &Arc<dyn ObjectStore>, path: &str) -> Result<()> {
    let paths: Vec<Path> = os
        .list(Some(&Path::from(path)))
        .map_ok(|m| m.location)
        .try_collect()
        .await?;
    for chunk in paths.chunks(128) {
        let owned: Vec<Path> = chunk.to_vec();
        let batch = futures::stream::iter(owned.into_iter().map(Ok));
        os.delete_stream(futures::StreamExt::boxed(batch))
            .try_collect::<Vec<_>>()
            .await?;
    }
    Ok(())
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
    let params = EngineParams {
        leaf_bytes: args.leaf_kib << 10,
        internal_bytes: args.internal_kib << 10,
        fanout: args.fanout,
        memtable_bytes: args.memtable_mib << 20,
        inner_cache_bytes: args.inner_cache_mib << 20,
        leaf_cache_bytes: args.leaf_cache_mib << 20,
        clean_below_live: args.clean_below,
        clean_inner_below_live: args.clean_inner_below,
        gc: (!args.no_gc).then_some(args.gc_deletes_per_sec),
        poll: Duration::from_millis(args.poll_ms),
        max_runs: args.max_runs,
        compression: args.compression,
        warm: if args.warm_background {
            sidra::Warm::Background
        } else {
            sidra::Warm::Blocking
        },
        // Scanned blocks are cached, as sidra caches leaves; SlateDB's
        // default leaves them out of the block cache.
        slatedb_scan: slatedb::config::ScanOptions::default()
            .with_cache_blocks(true)
            .with_read_ahead_bytes(args.slatedb_read_ahead_kib << 10)
            .with_max_fetch_tasks(args.slatedb_fetch_tasks),
    };
    let writers = match kind {
        EngineKind::Sidra => args.writers,
        EngineKind::Slatedb => 1,
    };
    let c = args.concurrency;
    let mut phases = Vec::new();
    let mut io_mark = counters.snapshot();
    let mut record = |name, ops, elapsed, hist, phases: &mut Vec<Phase>| {
        let now = counters.snapshot();
        let by = (now - io_mark).put_bytes_by;
        if by.iter().sum::<u64>() > 1 << 20 {
            let parts: Vec<String> = metered::KIND_NAMES
                .iter()
                .zip(by)
                .filter(|(_, b)| *b > 0)
                .map(|(n, b)| format!("{n} {:.1}", b as f64 / (1 << 20) as f64))
                .collect();
            eprintln!(
                "[{}] {name} PUT MiB by kind: {}",
                kind.name(),
                parts.join(", ")
            );
        }
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
    // One pass over every key, so the warm phases find the data cached in
    // both engines: sidra caches what it writes, SlateDB doesn't cache
    // compaction output.
    let (elapsed, hist, n) = timed(db.warm_up(wl.key(0), wl.keys as usize)).await?;
    ensure!(n == wl.keys as usize, "warm-up scan returned {n} keys");
    record("warmup", 1, elapsed, hist, &mut phases);

    let (elapsed, hist) = read_phase(1, db.clone(), 0).await?;
    record("read-warm", args.reads, elapsed, hist, &mut phases);

    let scan_phase = |seed: u64, db: Arc<Engine>| {
        let (wl, len) = (wl.clone(), args.scan_len);
        run_ops(args.scans, c, move |i| {
            let (db, wl) = (db.clone(), wl.clone());
            async move {
                let start = wl.random_id(seed, i).min(wl.keys - len as u64);
                let n = db.scan(wl.key(start), len).await?;
                ensure!(n == len, "scan returned {n}, expected {len}");
                Ok(())
            }
        })
    };
    let (elapsed, hist) = scan_phase(2, db.clone()).await?;
    record("scan", args.scans, elapsed, hist, &mut phases);

    let reopen = |db: Arc<Engine>| async {
        let db = Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("engine still shared"))?;
        let (elapsed, hist, ()) = timed(db.close()).await?;
        let (elapsed_open, hist_open, db) =
            timed(Engine::open(kind, os.clone(), &path, &params, writers)).await?;
        anyhow::Ok(((elapsed, hist), (elapsed_open, hist_open), Arc::new(db)))
    };
    let ((elapsed, hist), (elapsed_open, hist_open), db) = reopen(db).await?;
    record("close", 1, elapsed, hist, &mut phases);
    record("reopen", 1, elapsed_open, hist_open, &mut phases);

    let (elapsed, hist) = read_phase(3, db.clone(), 0).await?;
    record("read-cold", args.reads, elapsed, hist, &mut phases);

    // Reopened again so no leaf is cached: read-cold warmed most of them.
    let (_, (elapsed, hist), db) = reopen(db).await?;
    record("reopen", 1, elapsed, hist, &mut phases);
    let (elapsed, hist) = scan_phase(6, db.clone()).await?;
    record("scan-cold", args.scans, elapsed, hist, &mut phases);

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

    if args.mixed_secs > 0 {
        let mixed = mixed_phase(args, kind, db.clone(), wl.clone(), &counters).await?;
        mixed
            .into_iter()
            .for_each(|(name, ops, elapsed, hist)| record(name, ops, elapsed, hist, &mut phases));
    }

    let db = match args.delete_pct {
        0 => db,
        pct => {
            let reopen = || Engine::open(kind, os.clone(), &path, &params, writers);
            delete_phases(
                args,
                kind,
                db,
                wl.clone(),
                pct,
                reopen,
                &mut |name, ops, elapsed, hist| record(name, ops, elapsed, hist, &mut phases),
            )
            .await?
        }
    };

    let db = Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("engine still shared"))?;
    db.close().await?;
    let (objects, bytes) = footprint(&raw, &path).await?;
    eprintln!(
        "[{}] footprint: {objects} objects, {:.1} MiB (logical data {:.1} MiB)",
        kind.name(),
        bytes as f64 / (1 << 20) as f64,
        (wl.keys as usize * (wl.value_size + 15)) as f64 / (1 << 20) as f64
    );
    if !args.keep_data
        && let Err(e) = remove_all(&raw, &path).await
    {
        eprintln!("[{}] cleanup of {path} failed: {e:#}", kind.name());
    }
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
    let wl = Arc::new(Workload::new(args.keys, args.value_size, args.values));
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
