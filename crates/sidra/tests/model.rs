//! Model-based tests: random op sequences against a `BTreeMap`, with tiny
//! nodes so splits, buffer flushes, removals, relocation and root
//! growth/shrink happen constantly. Writers are replaced, crash mid-
//! checkpoint, and are fenced while still running; readers follow along.
//!
//! `simulation_*` runs the same harness deterministically in virtual time
//! (see the section below).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use proptest::prelude::*;
use sidra::{
    CacheConfig, CheckpointConfig, CleanerConfig, Clock, Db, DbReader, Durability, Error,
    ErrorKind, Freshness, Gc, GcConfig, KeyRange, ReadOptions, ReaderSettings, SegmentId, Settings,
    TreeConfig, Ts, WalConfig, Warm, WriteBatch, WriteOptions,
};

#[path = "support/faulty.rs"]
mod faulty;
use faulty::{Faults, Faulty, Trace};

const ROOT: &str = "db";
const HOUR: Duration = Duration::from_secs(3600);
const LATEST: ReadOptions = ReadOptions {
    freshness: Freshness::Latest,
};

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(1_000_000)))
    }
    fn tick(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn advance(&self, d: Duration) {
        self.0.fetch_add(d.as_millis() as u64, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// tokio's clock, for runs on a paused current-thread runtime: background
/// loops run as virtual time passes, and a seed replays exactly.
struct VirtualClock {
    start: tokio::time::Instant,
}

impl VirtualClock {
    /// Virtual time starts here, so lagging clocks stay positive.
    const EPOCH_MS: u64 = 1_000_000;

    fn new() -> Arc<Self> {
        Arc::new(Self {
            start: tokio::time::Instant::now(),
        })
    }
}

impl Clock for VirtualClock {
    fn now_ms(&self) -> u64 {
        Self::EPOCH_MS + self.start.elapsed().as_millis() as u64
    }
}

/// How time passes in a run.
#[derive(Clone)]
enum Time {
    /// Moves only when the test moves it.
    Manual(Arc<ManualClock>),
    Virtual(Arc<VirtualClock>),
}

impl Time {
    fn clock(&self) -> Arc<dyn Clock> {
        match self {
            Time::Manual(c) => c.clone(),
            Time::Virtual(c) => c.clone(),
        }
    }

    fn tick(&self) {
        match self {
            Time::Manual(c) => c.tick(),
            Time::Virtual(_) => {}
        }
    }

    async fn wait(&self, d: Duration) {
        match self {
            Time::Manual(c) => c.advance(d),
            Time::Virtual(_) => tokio::time::sleep(d).await,
        }
    }
}

/// A process clock running `lag_ms` behind the shared one.
struct Skewed {
    base: Arc<dyn Clock>,
    lag_ms: u64,
}

impl Clock for Skewed {
    fn now_ms(&self) -> u64 {
        self.base.now_ms() - self.lag_ms
    }
}

fn tiny_config() -> Settings {
    Settings {
        tree: TreeConfig {
            leaf_max_bytes: 200,
            internal_max_bytes: 400,
            max_fanout: 4,
            max_runs_per_child: 2,
            // Purge as soon as allowed, so purges happen constantly.
            purge_tombstones_after: Duration::ZERO,
            // A few leaves' worth, so purges often span checkpoints.
            purge_bytes_per_checkpoint: 1000,
        },
        // Small caches force reloads from storage.
        cache: CacheConfig {
            inner_bytes: 4096,
            leaf_bytes: 1024,
        },
        wal: WalConfig {
            flush_interval: Duration::from_millis(2),
            max_bytes: 256,
        },
        // Manual-clock durations: these never elapse unless a test advances.
        checkpoint: CheckpointConfig {
            bytes: 400,
            interval: HOUR,
            // A few batches, so backlogs often span checkpoints.
            max_bytes: 1000,
            max_unapplied_bytes: 1 << 30,
        },
        gc: Gc::Off,
        // Clean eagerly so relocation runs constantly.
        cleaner: CleanerConfig {
            clean_below_live: 0.6,
            clean_inner_below_live: 0.6,
            max_moved_bytes: 2048,
        },
        // Exercise the compressed path everywhere in tests.
        compression: sidra::Compression::Lz4,
        warm: Warm::Blocking,
        hedge: sidra::Hedge::default(),
    }
}

fn reader_config() -> ReaderSettings {
    ReaderSettings {
        cache: CacheConfig {
            inner_bytes: 4096,
            leaf_bytes: 1024,
        },
        refresh_interval: None,
        warm: Warm::Off,
        rewarm: Warm::Off,
        origin: None,
        hedge: sidra::Hedge::default(),
    }
}

async fn open(os: &Arc<dyn ObjectStore>, clock: &Arc<ManualClock>) -> Db {
    open_with(os, clock, tiny_config()).await
}

async fn open_with(os: &Arc<dyn ObjectStore>, clock: &Arc<ManualClock>, cfg: Settings) -> Db {
    Db::builder(ROOT, os.clone())
        .with_settings(cfg)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap()
}

async fn reader(os: &Arc<dyn ObjectStore>) -> DbReader {
    DbReader::builder(ROOT, os.clone())
        .with_settings(reader_config())
        .build()
        .await
        .unwrap()
}

/// The manifest's live bytes per segment equal the bytes of reachable nodes.
async fn assert_accounting(db: &Db) {
    let audit = until_ok("audit", || db.audit()).await;
    let reachable = audit
        .shape
        .nodes
        .iter()
        .fold(BTreeMap::<SegmentId, u64>::new(), |mut m, p| {
            *m.entry(p.seg).or_default() += u64::from(p.len);
            m
        });
    let table: BTreeMap<SegmentId, u64> =
        audit.segments.iter().map(|(id, u)| (*id, u.live)).collect();
    assert_eq!(table, reachable, "segment accounting");
}

#[derive(Clone, Debug)]
enum Op {
    Put(u8, Vec<u8>),
    PutQueued(u8, Vec<u8>),
    Delete(u8),
    Get(u8),
    Scan(u8, u8),
    Checkpoint,
    /// A fresh writer replaces the current one, which then closes. Its
    /// clock runs this many ms behind.
    Reopen(u64),
    /// The writer dies without closing, this many microseconds into a
    /// checkpoint. Its replacement's clock lags by the second value (ms).
    Crash(u64, u64),
    /// A fresh writer opens while the current one still runs; the old one
    /// then tries to write and checkpoint, and must fail without effect.
    Fence(u8, u64),
    ReaderScan,
    /// A read through a long-lived reader that refreshes on its own timer,
    /// so it may still be on an old manifest. It must not fail: GC keeps
    /// what such a reader needs for the retention period.
    StaleGet(u8),
    /// The long-lived reader with `Freshness::Latest` sees every
    /// acknowledged write.
    LatestGet(u8),
    /// Time passes (ms) with nothing asked of the writer; its background
    /// loops (checkpoints, purges, GC) keep running.
    Wait(u64),
}

impl Op {
    /// Whether the op checks reads against the model.
    fn reads(&self) -> bool {
        match self {
            Op::Get(_) | Op::Scan(..) | Op::ReaderScan | Op::LatestGet(_) => true,
            Op::Put(..)
            | Op::PutQueued(..)
            | Op::Delete(_)
            | Op::Checkpoint
            | Op::Reopen(_)
            | Op::Crash(..)
            | Op::Fence(..)
            | Op::StaleGet(_)
            | Op::Wait(_) => false,
        }
    }
}

fn key(k: u8) -> Bytes {
    Bytes::from(format!("key{k:03}"))
}

fn op() -> impl Strategy<Value = Op> {
    let value = proptest::collection::vec(any::<u8>(), 0..40);
    prop_oneof![
        6 => (0u8..60, value.clone()).prop_map(|(k, v)| Op::Put(k, v)),
        3 => (0u8..60, value).prop_map(|(k, v)| Op::PutQueued(k, v)),
        4 => (0u8..60).prop_map(Op::Delete),
        2 => (0u8..60).prop_map(Op::Get),
        1 => (0u8..60, 0u8..60).prop_map(|(a, b)| Op::Scan(a, b)),
        2 => Just(Op::Checkpoint),
        1 => (0u64..3_000).prop_map(Op::Reopen),
        1 => (0u64..3000, 0u64..3_000).prop_map(|(us, lag)| Op::Crash(us, lag)),
        1 => (0u8..60, 0u64..3_000).prop_map(|(k, lag)| Op::Fence(k, lag)),
        1 => Just(Op::ReaderScan),
        1 => (0u8..60).prop_map(Op::StaleGet),
        1 => (0u8..60).prop_map(Op::LatestGet),
    ]
}

type Model = BTreeMap<Bytes, Bytes>;

/// Last-writer-wins by timestamp. Timestamps grow across writers, so this
/// is also call order.
#[derive(Default)]
struct Lww {
    map: BTreeMap<Bytes, (Ts, Option<Bytes>)>,
    last: Option<Ts>,
}

impl Lww {
    fn apply(&mut self, k: Bytes, ts: Ts, v: Option<Bytes>) {
        assert!(
            self.last.is_none_or(|l| ts > l),
            "timestamp {ts} not after {:?}",
            self.last
        );
        self.last = Some(ts);
        self.map.insert(k, (ts, v));
    }

    fn visible(&self) -> Model {
        self.map
            .iter()
            .filter_map(|(k, (_, v))| v.clone().map(|v| (k.clone(), v)))
            .collect()
    }
}

fn model_range(model: &Model, range: &KeyRange) -> Vec<(Bytes, Bytes)> {
    model
        .range((range.start.clone(), range.end.clone()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Whether `e` is a fault the store injected, the only failure a run
/// tolerates. Other store errors (an object GC deleted too early, say) are
/// bugs, even though they're retryable.
fn injected(e: &Error) -> bool {
    e.is_retryable() && format!("{e:?}").contains("injected fault")
}

/// Retries an operation that failed only because the store did (injected
/// faults); any other error fails the test.
async fn until_ok<T, Fut>(what: &str, mut f: impl FnMut() -> Fut) -> T
where
    Fut: std::future::Future<Output = sidra::Result<T>>,
{
    let mut last = None;
    for _ in 0..100 {
        match f().await {
            Ok(v) => return v,
            Err(e) if injected(&e) => last = Some(e),
            Err(e) => panic!("{what}: {e}"),
        }
    }
    panic!("{what}: still failing after 100 attempts: {last:?}");
}

/// What a run varies besides its ops.
struct Setup {
    /// `None`: a fault-free store.
    faults: Option<Faults>,
    seed: u64,
    time: Time,
    cfg: Settings,
}

/// One writer at a time on one store, optionally behind injected faults.
struct World {
    os: Arc<dyn ObjectStore>,
    /// The store under `os`; it injects faults only when `faults` is set.
    store: Arc<Faulty>,
    faults: bool,
    time: Time,
    cfg: Settings,
    db: Option<Db>,
    /// See `Op::StaleGet`.
    reader: DbReader,
}

impl World {
    async fn new(setup: Setup) -> Self {
        let store = Arc::new(Faulty::new(
            Arc::new(InMemory::new()),
            setup.faults.unwrap_or(Faults::NONE),
            setup.seed,
        ));
        let os: Arc<dyn ObjectStore> = store.clone();
        let mut w = Self {
            reader: until_ok("reader open", || {
                DbReader::builder(ROOT, os.clone())
                    .with_settings(ReaderSettings {
                        refresh_interval: Some(Duration::from_secs(1)),
                        ..reader_config()
                    })
                    .build()
            })
            .await,
            os,
            store,
            faults: setup.faults.is_some(),
            time: setup.time,
            cfg: setup.cfg,
            db: None,
        };
        w.db = Some(w.open(0).await);
        w
    }

    fn db(&self) -> &Db {
        self.db.as_ref().expect("a writer is open")
    }

    async fn open(&self, lag_ms: u64) -> Db {
        let clock = Arc::new(Skewed {
            base: self.time.clock(),
            lag_ms,
        });
        until_ok("open", || {
            Db::builder(ROOT, self.os.clone())
                .with_settings(self.cfg.clone())
                .with_clock(clock.clone())
                .build()
        })
        .await
    }

    /// Makes queued writes durable: only acknowledged writes must survive a
    /// writer being replaced.
    async fn flush(&mut self) {
        match self.db().flush_wal().await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::WriterLost => self.reopen(0).await,
            Err(e) => panic!("flush: {e}"),
        }
    }

    /// Replaces the writer with a fresh one and closes the old one, which
    /// finds itself fenced (or not, if it had nothing left to do).
    async fn reopen(&mut self, lag_ms: u64) {
        let fresh = self.open(lag_ms).await;
        let old = self.db.replace(fresh).expect("a writer is open");
        match old.close().await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::WriterLost => {}
            Err(e) if self.faults && injected(&e) => {}
            Err(e) => panic!("close: {e}"),
        }
    }

    /// Writes until acknowledged, replacing the writer if it's lost (then
    /// the write may have landed already; repeating it is harmless).
    async fn write(&mut self, batch: WriteBatch, opts: WriteOptions) -> Ts {
        for _ in 0..100 {
            match self.db().write_with(batch.clone(), opts).await {
                Ok(ts) => return ts.expect("non-empty batch"),
                Err(e) if injected(&e) => {}
                Err(e) if e.kind() == ErrorKind::WriterLost => self.reopen(0).await,
                Err(e) => panic!("write: {e}"),
            }
        }
        panic!("write: still failing");
    }

    /// Kills the writer part-way through a checkpoint and opens a fresh one.
    async fn crash(&mut self, after: Duration, lag_ms: u64) {
        self.flush().await;
        // Cancelling the checkpoint stops it at whatever await it reached:
        // segments may be uploaded with no manifest.
        let _ = tokio::time::timeout(after, self.db().checkpoint()).await;
        drop(self.db.take());
        self.db = Some(self.open(lag_ms).await);
    }

    /// Opens a writer while the current one runs. The old one can't write,
    /// checkpoint, or change anything after that.
    async fn fence(&mut self, k: u8, lag_ms: u64) {
        self.flush().await;
        let fresh = self.open(lag_ms).await;
        let old = self.db.replace(fresh).expect("a writer is open");
        let mut batch = WriteBatch::new();
        batch.put(key(k), "zombie");
        let wrote = old.write(batch).await;
        assert!(
            matches!(&wrote, Err(e) if e.kind() == ErrorKind::WriterLost || injected(e)),
            "a fenced writer wrote: {wrote:?}"
        );
        let checkpointed = old.checkpoint().await;
        assert!(
            matches!(&checkpointed, Err(e) if e.kind() == ErrorKind::WriterLost || injected(e)),
            "a fenced writer checkpointed: {checkpointed:?}"
        );
        drop(old);
    }

    async fn checkpoint(&mut self) {
        match self.db().checkpoint().await {
            Ok(()) => assert_accounting(self.db()).await,
            Err(e) if injected(&e) => {}
            Err(e) if self.faults && e.kind() == ErrorKind::WriterLost => self.reopen(0).await,
            Err(e) => panic!("checkpoint: {e}"),
        }
    }
}

/// Runs `ops`, checking reads against the model as it goes and everything
/// at the end. Returns the store's request trace.
async fn run(ops: Vec<Op>, setup: Setup) -> Trace {
    let mut world = World::new(setup).await;
    let mut lww = Lww::default();
    // Queued writes not yet flushed: they turn visible with the next flush.
    let mut queued = false;
    for op in ops {
        if queued && op.reads() {
            world.flush().await;
            queued = false;
        }
        match op {
            Op::Put(k, v) => {
                world.time.tick();
                let mut batch = WriteBatch::new();
                batch.put(key(k), &v);
                let ts = world.write(batch, WriteOptions::default()).await;
                lww.apply(key(k), ts, Some(Bytes::from(v)));
            }
            Op::PutQueued(k, v) => {
                world.time.tick();
                let mut batch = WriteBatch::new();
                batch.put(key(k), &v);
                // A queued write is lost if its writer is lost before the
                // flush, so under faults only acknowledged writes count.
                let durability = match world.faults {
                    true => Durability::AwaitDurable,
                    false => Durability::Queued,
                };
                let ts = world.write(batch, WriteOptions { durability }).await;
                lww.apply(key(k), ts, Some(Bytes::from(v)));
                queued = true;
            }
            Op::Delete(k) => {
                world.time.tick();
                let mut batch = WriteBatch::new();
                batch.delete(key(k));
                let ts = world.write(batch, WriteOptions::default()).await;
                lww.apply(key(k), ts, None);
            }
            Op::Get(k) => {
                let got = until_ok("get", || world.db().get(key(k))).await;
                assert_eq!(got, lww.visible().get(&key(k)).cloned(), "get {k}");
            }
            Op::Scan(a, b) => {
                let model = lww.visible();
                let (lo, hi) = (a.min(b), a.max(b));
                let ranges = [
                    KeyRange::new(Bound::Included(key(lo)), Bound::Excluded(key(hi))),
                    KeyRange::new(Bound::Excluded(key(lo)), Bound::Included(key(hi))),
                    KeyRange::new(Bound::Unbounded, Bound::Included(key(hi))),
                    KeyRange::new(Bound::Included(key(lo)), Bound::Unbounded),
                ];
                for r in ranges {
                    let got = until_ok("scan", || world.db().scan(r.clone()).collect()).await;
                    assert_eq!(got, model_range(&model, &r), "scan {r:?}");
                }
            }
            Op::Checkpoint => world.checkpoint().await,
            Op::Reopen(lag) => {
                world.flush().await;
                world.reopen(lag).await
            }
            Op::Crash(us, lag) => world.crash(Duration::from_micros(us), lag).await,
            Op::Fence(k, lag) => world.fence(k, lag).await,
            Op::Wait(ms) => world.time.wait(Duration::from_millis(ms)).await,
            Op::StaleGet(k) => match world.reader.get(key(k)).await {
                Ok(_) => {}
                Err(e) if injected(&e) => {}
                Err(e) => panic!("stale get {k}: {e}"),
            },
            Op::LatestGet(k) => {
                world.flush().await;
                let got = until_ok("latest get", || world.reader.get_with(key(k), LATEST)).await;
                assert_eq!(got, lww.visible().get(&key(k)).cloned(), "latest get {k}");
            }
            Op::ReaderScan => {
                world.flush().await;
                let model = lww.visible();
                let fresh = until_ok("reader open", || {
                    DbReader::builder(ROOT, world.os.clone())
                        .with_settings(reader_config())
                        .build()
                })
                .await;
                let got = until_ok("reader scan", || fresh.scan(KeyRange::all()).collect()).await;
                assert_eq!(got, model_range(&model, &KeyRange::all()), "reader scan");
            }
        }
    }
    // Verify without faults: everything acknowledged survived them.
    world.store.set_enabled(false);
    world.flush().await;
    let model = lww.visible();
    let all = world.db().scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
    world.checkpoint().await;
    world.db.take().unwrap().close().await.unwrap();
    let db = world.open(0).await;
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()), "after reopen");
    db.close().await.unwrap();
    world.store.trace()
}

/// `default` cases, or `PROPTEST_CASES` for stress runs (an explicit
/// `cases` would otherwise override it).
fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(48), ..ProptestConfig::default() })]
    #[test]
    fn matches_model(ops in proptest::collection::vec(op(), 1..250), seed in any::<u64>()) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let setup = Setup { faults: None, seed, time: Time::Manual(ManualClock::new()), cfg: tiny_config() };
        rt.block_on(run(ops, setup));
    }
}

proptest! {
    // Each retried fault sleeps for the backoff, so cases are slow.
    #![proptest_config(ProptestConfig { cases: cases(12), ..ProptestConfig::default() })]
    /// Same, with store requests failing before and after they apply, and
    /// random delays.
    #[test]
    fn matches_model_under_faults(ops in proptest::collection::vec(op(), 1..150), seed in any::<u64>()) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let faults = Faults {
            fail_before: 0.05,
            fail_after: 0.05,
            max_delay: Duration::from_millis(1),
        };
        let setup = Setup { faults: Some(faults), seed, time: Time::Manual(ManualClock::new()), cfg: tiny_config() };
        rt.block_on(run(ops, setup));
    }
}

// ---------------------------------------------------------------------------
// Deterministic simulation: the same harness on a paused current-thread
// runtime, so virtual time drives every timer and a seed replays exactly.
// Timers are realistic (seconds, not hours) and waits let minutes pass, so
// background checkpoints, purges and GC all happen.

const SIM_FAULTS: Faults = Faults {
    fail_before: 0.03,
    fail_after: 0.03,
    max_delay: Duration::from_millis(20),
};

fn sim_config() -> Settings {
    let tiny = tiny_config();
    Settings {
        tree: TreeConfig {
            purge_tombstones_after: Duration::from_secs(5),
            ..tiny.tree
        },
        wal: WalConfig {
            flush_interval: Duration::from_millis(1),
            ..tiny.wal
        },
        checkpoint: CheckpointConfig {
            interval: Duration::from_secs(1),
            ..tiny.checkpoint
        },
        gc: Gc::On(GcConfig {
            interval: Duration::from_secs(5),
            retention: Duration::from_secs(60),
            max_deletes_per_sec: 1000,
        }),
        ..tiny
    }
}

/// The model test's ops plus waits: mostly short, sometimes past the GC
/// retention.
fn sim_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        20 => op(),
        3 => (0u64..2_000).prop_map(Op::Wait),
        1 => (2_000u64..90_000).prop_map(Op::Wait),
    ]
}

/// Runs `ops` in a fresh simulated world, on a store that injects `faults`.
fn simulate(ops: Vec<Op>, faults: Option<Faults>, seed: u64) -> Trace {
    virtual_runtime().block_on(async {
        let setup = Setup {
            faults,
            seed,
            time: Time::Virtual(VirtualClock::new()),
            cfg: sim_config(),
        };
        run(ops, setup).await
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(128), ..ProptestConfig::default() })]
    #[test]
    fn simulation_matches_model(
        ops in proptest::collection::vec(sim_op(), 1..200),
        faults in prop_oneof![Just(None), Just(Some(SIM_FAULTS))],
        seed in any::<u64>(),
    ) {
        simulate(ops, faults, seed);
    }
}

/// A paused current-thread runtime, for `VirtualClock` runs. Time only: the
/// IO driver (unused, the store is in memory) parks for real between timers
/// even with time paused, 40x slower.
fn virtual_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Writes that outpace checkpoints stall at the backlog limit instead of
/// failing or growing it, and checkpoints keep publishing.
#[test]
fn overload_stalls_writes_while_checkpoints_keep_up() {
    const LIMIT: usize = 100_000;
    const RUN: Duration = Duration::from_secs(300);
    virtual_runtime().block_on(async {
        let store = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
        // Uploads are slow next to how fast the writes come.
        store.throttle_puts(|p| p.ends_with(".seg"), 800);
        let os: Arc<dyn ObjectStore> = store.clone();
        let mut cfg = sim_config();
        cfg.checkpoint.max_unapplied_bytes = LIMIT;
        let db = Arc::new(
            Db::builder(ROOT, os)
                .with_settings(cfg)
                .with_clock(VirtualClock::new())
                .build()
                .await
                .unwrap(),
        );
        let start = tokio::time::Instant::now();
        let tasks = (0..3).map(|t| {
            let db = db.clone();
            tokio::spawn(async move {
                // Last value written per key.
                let mut wrote = BTreeMap::new();
                for n in 0u32.. {
                    if start.elapsed() >= RUN {
                        break;
                    }
                    let (k, v) = (format!("t{t}-{:03}", n % 500), format!("{n:0100}"));
                    // Stalled, not stuck.
                    tokio::time::timeout(RUN, db.put(k.clone(), v.clone()))
                        .await
                        .expect("write stalled the whole run")
                        .unwrap();
                    wrote.insert(k, v);
                }
                wrote
            })
        });
        let tasks = futures::future::join_all(tasks);
        tokio::pin!(tasks);
        let (mut backlog, mut gap) = (0, Duration::ZERO);
        let mut last_manifest = (db.metrics().manifest_id, start.elapsed());
        let results = loop {
            tokio::select! {
                biased;
                r = &mut tasks => break r,
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            let m = db.metrics();
            backlog = backlog.max(m.unapplied_bytes);
            if m.manifest_id != last_manifest.0 {
                last_manifest = (m.manifest_id, start.elapsed());
            }
            gap = gap.max(start.elapsed() - last_manifest.1);
        };
        eprintln!("backlog {backlog} B, manifest gap {gap:?}");
        assert!(gap <= Duration::from_secs(15), "no manifest for {gap:?}");
        assert!(backlog <= 2 * LIMIT, "backlog reached {backlog} B");
        db.checkpoint().await.unwrap();
        assert_eq!(db.metrics().unapplied_bytes, 0);
        for wrote in results.into_iter().map(Result::unwrap) {
            for (k, v) in wrote {
                assert_eq!(
                    db.get(&k).await.unwrap().as_deref(),
                    Some(v.as_bytes()),
                    "{k}"
                );
            }
        }
    });
}

/// A seed replays the same store requests, so a failing case reproduces
/// and shrinks.
#[test]
fn simulation_replays_exactly() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let ops = proptest::collection::vec(sim_op(), 150)
        .new_tree(&mut runner)
        .unwrap()
        .current();
    let faults = Some(SIM_FAULTS);
    let first = simulate(ops.clone(), faults, 7);
    assert!(first.requests > 1000, "{first:?}");
    assert_eq!(simulate(ops, faults, 7), first);
}

// ---------------------------------------------------------------------------
// Scenarios

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_load_builds_multi_level_tree() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&os, &clock).await;
    let mut model = Model::new();
    for i in 0..2000u32 {
        clock.tick();
        let k = Bytes::from(format!("k{:08}", (i * 7919) % 2000));
        let v = Bytes::from(format!("v{i}"));
        db.put(k.clone(), v.clone()).await.unwrap();
        model.insert(k, v);
    }
    db.checkpoint().await.unwrap();
    let height = db.metrics().tree.unwrap().height;
    assert!(height >= 3, "height {height}");
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

/// Concurrent writes share one log in timestamp order, so a checkpoint that
/// applies only a prefix of it never leaves an older write behind a newer
/// delete it dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_land_in_timestamp_order() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = tiny_config();
    // One batch per checkpoint, and nothing in the background.
    cfg.checkpoint.max_bytes = 1;
    cfg.checkpoint.bytes = 1 << 30;
    let db = Arc::new(
        Db::builder(ROOT, os.clone())
            .with_settings(cfg)
            .build()
            .await
            .unwrap(),
    );
    let tasks: Vec<_> = (0..8u32)
        .map(|t| {
            let db = db.clone();
            tokio::spawn(async move {
                let mut wrote = Vec::new();
                for i in 0..100u32 {
                    let mut batch = WriteBatch::new();
                    let value = match (t + i) % 3 {
                        0 => None,
                        _ => Some(format!("{t}:{i}")),
                    };
                    match &value {
                        Some(v) => batch.put("k", v),
                        None => batch.delete("k"),
                    };
                    batch.put(format!("t{t}-{i:03}"), "v");
                    wrote.push((db.write(batch).await.unwrap().unwrap(), value));
                }
                wrote
            })
        })
        .collect();
    let wrote: Vec<(Ts, Option<String>)> = futures::future::try_join_all(tasks)
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    let (_, last) = wrote.iter().max_by_key(|(ts, _)| *ts).unwrap();
    db.checkpoint().await.unwrap();
    assert_eq!(db.get("k").await.unwrap(), last.clone().map(Bytes::from));
    let fresh = reader(&os).await;
    assert_eq!(fresh.get("k").await.unwrap(), last.clone().map(Bytes::from));
    assert_eq!(fresh.scan("t".."u").collect().await.unwrap().len(), 800);
}

/// A writer that dies without closing leaves durable writes in the log; the
/// next writer replays and applies them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_writer_replays_the_previous_writers_log() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    clock.tick();
    a.put("applied", "1").await.unwrap();
    a.checkpoint().await.unwrap();
    clock.tick();
    a.put("logged", "2").await.unwrap();
    clock.tick();
    a.delete("applied").await.unwrap();
    drop(a);
    let b = open(&os, &clock).await;
    assert_eq!(b.get("logged").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(b.get("applied").await.unwrap(), None);
    b.checkpoint().await.unwrap();
    assert_eq!(b.metrics().unapplied_bytes, 0);
    let fresh = reader(&os).await;
    assert_eq!(
        fresh.get("logged").await.unwrap().as_deref(),
        Some(&b"2"[..])
    );
    assert_eq!(fresh.get("applied").await.unwrap(), None);
}

/// Opening a writer fences the previous one: it can't write, checkpoint, or
/// stay unaware while idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_a_writer_fences_the_previous_one() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    // Also how often an idle writer checks for a newer one (real time).
    cfg.checkpoint.interval = Duration::from_millis(20);
    let a = open_with(&os, &clock, cfg.clone()).await;
    let idle = open_with(&os, &clock, cfg.clone()).await;
    clock.tick();
    idle.put("x", "1").await.unwrap();
    let b = open_with(&os, &clock, cfg).await;
    assert!(b.epoch() > idle.epoch());
    assert!(matches!(a.put("y", "1").await, Err(Error::Fenced)));
    assert!(matches!(a.checkpoint().await, Err(Error::Fenced)));
    assert!(matches!(a.status(), Err(Error::Fenced)));
    wait_until("an idle writer notices", || idle.status().is_err()).await;
    assert_eq!(b.get("x").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(b.get("y").await.unwrap(), None);
}

/// Faulty store with log PUTs held in flight, and a writer on it. The
/// writer doesn't hedge, so the held PUT is its only copy.
async fn held_writer() -> (Arc<Faulty>, Arc<ManualClock>, Arc<Db>, Arc<faulty::Hold>) {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let os: Arc<dyn ObjectStore> = faulty.clone();
    let clock = ManualClock::new();
    let cfg = Settings {
        hedge: sidra::Hedge::Off,
        ..tiny_config()
    };
    let a = Arc::new(open_with(&os, &clock, cfg).await);
    let log = faulty.hold_puts(|p| p.ends_with(".wal"), 1);
    (faulty, clock, a, log)
}

/// A stalled log PUT gets a second copy, which lands and acks the write;
/// without it the write would wait for the release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_log_put_is_hedged() {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let os: Arc<dyn ObjectStore> = faulty.clone();
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    let log = faulty.hold_puts(|p| p.ends_with(".wal"), 1);
    clock.tick();
    a.put("k", "v").await.unwrap();
    assert_eq!(log.arrived(), 1);
    assert_eq!(a.metrics().requests.log_hedges, 1);
    log.release();
    a.put("k2", "v").await.unwrap();
    let b = open(&os, &clock).await;
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(b.get("k2").await.unwrap().as_deref(), Some(&b"v"[..]));
}

/// A log write still in flight when a newer writer opens lands on the
/// fence and fails: nothing is acknowledged that the new writer lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_in_flight_write_of_a_fenced_writer_is_not_acknowledged() {
    let (faulty, clock, a, log) = held_writer().await;
    let os: Arc<dyn ObjectStore> = faulty.clone();
    clock.tick();
    let pending = tokio::spawn({
        let a = a.clone();
        async move { a.put("k", "lost").await }
    });
    wait_until("log write in flight", || log.arrived() == 1).await;
    let b = open(&os, &clock).await;
    log.release();
    assert!(matches!(pending.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(b.get("k").await.unwrap(), None);
}

/// Fences outlive GC: a fenced writer's put that arrives after the log
/// around it was collected still finds the fence and fails, instead of
/// landing where no reader looks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fences_survive_gc() {
    let (faulty, clock, a, log) = held_writer().await;
    let os: Arc<dyn ObjectStore> = faulty.clone();
    clock.tick();
    let pending = tokio::spawn({
        let a = a.clone();
        async move { a.put("k", "lost").await }
    });
    wait_until("log write in flight", || log.arrived() == 1).await;
    let b = open(&os, &clock).await;
    for step in 0..3u32 {
        clock.advance(Duration::from_secs(120));
        b.put(format!("z{step}"), "v").await.unwrap();
        b.checkpoint().await.unwrap();
    }
    let stats = b.run_gc(Duration::from_secs(60)).await.unwrap();
    assert!(stats.wals > 0, "{stats:?}");
    let log_info = sidra::Admin::new(ROOT, os.clone()).log().await.unwrap();
    assert_eq!(log_info.fences, 2, "{log_info:?}");
    log.release();
    assert!(matches!(pending.await.unwrap(), Err(Error::Fenced)));
    assert_eq!(b.get("k").await.unwrap(), None);
}

/// Writes and checkpoints on `db` while the clock moves past a 60 s
/// retention, then collects.
async fn churn_and_collect(db: &Db, clock: &ManualClock) -> sidra::GcStats {
    for step in 0..3u32 {
        clock.advance(Duration::from_secs(120));
        db.put(format!("z{step}"), "v").await.unwrap();
        db.checkpoint().await.unwrap();
    }
    db.run_gc(Duration::from_secs(60)).await.unwrap()
}

/// A writer stalls past the GC retention with its checkpoint's manifest in
/// flight, so the put lands in an id GC already collected. The writer finds
/// a newer manifest listed and stops; GC deletes the stray without acting
/// on its dead segments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_stalled_past_gc_retention_is_a_stray() {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let os: Arc<dyn ObjectStore> = faulty.clone();
    let clock = ManualClock::new();
    let a = Arc::new(open(&os, &clock).await);
    a.put("k", "a").await.unwrap();
    let publish = faulty.hold_puts(|p| p.ends_with(".mf"), 1);
    let pending = tokio::spawn({
        let a = a.clone();
        async move { a.checkpoint().await }
    });
    wait_until("manifest put in flight", || publish.arrived() == 1).await;
    let b = open(&os, &clock).await;
    let stats = churn_and_collect(&b, &clock).await;
    assert!(stats.manifests >= 2, "{stats:?}");
    publish.release();
    assert!(matches!(pending.await.unwrap(), Err(Error::Fenced)));
    let stray = objects(&os, "manifest").await;
    let stats = b.run_gc(Duration::from_secs(60)).await.unwrap();
    assert_eq!(stats.manifests, 1, "{stats:?}");
    assert_eq!(objects(&os, "manifest").await, stray - 1);
    assert_eq!(b.get("k").await.unwrap().as_deref(), Some(&b"a"[..]));
    assert_accounting(&b).await;
}

/// An opener stalls past the GC retention with its claim in flight, so the
/// claim lands in an id GC already collected. It finds a newer manifest
/// listed and fails as fenced, instead of opening on the old tree and
/// deleting the live writer's segments as orphans. The live writer goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_opener_stalled_past_gc_retention_is_fenced() {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let os: Arc<dyn ObjectStore> = faulty.clone();
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    a.put("k", "a").await.unwrap();
    a.checkpoint().await.unwrap();
    let claim = faulty.hold_puts(|p| p.ends_with(".mf"), 1);
    let opening = tokio::spawn({
        let (os, clock) = (os.clone(), clock.clone());
        async move {
            Db::builder(ROOT, os)
                .with_settings(tiny_config())
                .with_clock(clock)
                .build()
                .await
        }
    });
    wait_until("claim in flight", || claim.arrived() == 1).await;
    let stats = churn_and_collect(&a, &clock).await;
    assert!(stats.manifests >= 2, "{stats:?}");
    claim.release();
    assert!(matches!(opening.await.unwrap(), Err(Error::Fenced)));
    a.put("late", "a").await.unwrap();
    let c = open(&os, &clock).await;
    for (k, v) in [
        ("k", "a"),
        ("z0", "v"),
        ("z1", "v"),
        ("z2", "v"),
        ("late", "a"),
    ] {
        assert_eq!(
            c.get(k).await.unwrap().as_deref(),
            Some(v.as_bytes()),
            "{k}"
        );
    }
    assert_accounting(&c).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstones_go_once_they_reach_leaves() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&os, &clock).await;
    for i in 0..300u32 {
        clock.tick();
        db.put(format!("k{i:04}"), "v").await.unwrap();
    }
    db.checkpoint().await.unwrap();
    for i in 0..300u32 {
        clock.tick();
        db.delete(format!("k{i:04}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    for round in 0..20u32 {
        clock.advance(Duration::from_secs(1));
        db.put(format!("z{round:04}"), "v").await.unwrap();
        db.checkpoint().await.unwrap();
    }
    let shape = db.tree_shape().await.unwrap();
    assert_eq!(
        shape.tombstones + shape.runs_with_tombstones,
        0,
        "{shape:?}"
    );
    assert_eq!(db.scan(KeyRange::all()).collect().await.unwrap().len(), 20);
}

/// Deletes buffered above a range nobody writes to again still reach their
/// leaves and go: the writer checkpoints for the purge with no new writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_tree_purges_buffered_tombstones() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.tree.purge_tombstones_after = Duration::from_secs(10);
    // How often an idle writer looks for work (real time).
    cfg.checkpoint.interval = Duration::from_millis(20);
    let db = open_with(&os, &clock, cfg).await;
    for i in 0..300u32 {
        clock.tick();
        db.put(format!("k{i:04}"), "v").await.unwrap();
    }
    db.checkpoint().await.unwrap();
    for i in 0..150u32 {
        clock.tick();
        db.delete(format!("k{i:04}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    let before = db.tree_shape().await.unwrap();
    assert!(before.runs_with_tombstones > 0, "{before:?}");

    clock.advance(Duration::from_secs(20));
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let shape = db.tree_shape().await.unwrap();
        if shape.runs_with_tombstones == 0 {
            break shape;
        }
        assert!(Instant::now() < deadline, "not purged: {shape:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(after.leaves < before.leaves, "{before:?} -> {after:?}");
    assert_eq!(db.scan(KeyRange::all()).collect().await.unwrap().len(), 150);
}

/// A purge bigger than `purge_bytes_per_checkpoint` spreads over more
/// checkpoints instead of rewriting everything at once. On paused time, so
/// the background checkpoints interleave the same way every run.
#[test]
fn purge_spreads_over_checkpoints() {
    let unbounded = virtual_runtime().block_on(checkpoints_to_purge(usize::MAX));
    // One purge per checkpoint.
    let bounded = virtual_runtime().block_on(checkpoints_to_purge(1));
    assert!(bounded > unbounded, "{bounded} vs {unbounded}");
}

/// Deletes half of 300 keys and counts the checkpoints that purge them.
async fn checkpoints_to_purge(budget: usize) -> usize {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.tree.purge_tombstones_after = Duration::from_secs(10);
    cfg.tree.purge_bytes_per_checkpoint = budget;
    // Only purges may carry tombstones down here: not the cleaner moving
    // live nodes, nor full buffers flushing (tiny ones cascade to the
    // leaves on any flush).
    cfg.cleaner.clean_below_live = 0.0;
    cfg.cleaner.clean_inner_below_live = 0.0;
    cfg.tree.internal_max_bytes = 1 << 20;
    cfg.tree.max_runs_per_child = 64;
    let db = open_with(&os, &clock, cfg).await;
    for i in 0..300u32 {
        clock.tick();
        db.put(format!("k{i:04}"), "v").await.unwrap();
    }
    db.checkpoint().await.unwrap();
    for i in 0..150u32 {
        clock.tick();
        db.delete(format!("k{i:04}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    // Every checkpoint counts, the background loop's too.
    let before = db.metrics().checkpoints;
    clock.advance(Duration::from_secs(20));
    while db.tree_shape().await.unwrap().runs_with_tombstones > 0 {
        assert!(
            db.metrics().checkpoints - before < 100,
            "purge makes no progress"
        );
        db.checkpoint().await.unwrap();
    }
    assert_eq!(db.scan(KeyRange::all()).collect().await.unwrap().len(), 150);
    (db.metrics().checkpoints - before) as usize
}

async fn objects(os: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    let prefix = Path::from(format!("{ROOT}/{prefix}"));
    os.list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_keeps_exactly_the_live_tree() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&os, &clock).await;
    let mut model = Model::new();
    for round in 0..20u32 {
        for i in 0..40u32 {
            clock.tick();
            let k = Bytes::from(format!("k{:04}", (i * 13 + round * 7) % 300));
            let v = Bytes::from(format!("r{round}i{i}"));
            db.put(k.clone(), v.clone()).await.unwrap();
            model.insert(k, v);
        }
        db.checkpoint().await.unwrap();
    }
    let before = objects(&os, "seg").await;

    // Within retention nothing is deleted.
    let stats = db.run_gc(Duration::from_secs(60)).await.unwrap();
    assert_eq!(stats.segments, 0);

    clock.advance(Duration::from_secs(120));
    let stats = db.run_gc(Duration::from_secs(60)).await.unwrap();
    assert!(
        stats.segments > 0 && stats.manifests > 0 && stats.wals > 0,
        "{stats:?}"
    );
    assert_accounting(&db).await;
    let after = objects(&os, "seg").await;
    assert!(after < before);
    assert_eq!(
        after,
        db.segment_table().len(),
        "only referenced segments remain"
    );
    // Only the writer's fence is left of the applied log.
    assert_eq!(objects(&os, "wal").await, 1);
    // The cleaner keeps space amplification bounded.
    let s = db.metrics();
    assert!(s.live_bytes * 10 >= s.segment_bytes * 4, "{s:?}");

    db.close().await.unwrap();
    let all = reader(&os)
        .await
        .scan(KeyRange::all())
        .collect()
        .await
        .unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_segments_are_deleted_on_open() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&os, &clock).await;
    for i in 0..200u32 {
        clock.tick();
        db.put(format!("k{i:04}"), format!("v{i}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    db.close().await.unwrap();
    let published = objects(&os, "seg").await;
    // A writer that uploaded a segment but died before publishing.
    let orphan = Path::from(format!("{ROOT}/seg/{:020}-{:020}.seg", 1, 1_000_000));
    os.put(&orphan, "junk".into()).await.unwrap();
    let db = open(&os, &clock).await;
    assert_eq!(objects(&os, "seg").await, published);
    assert_eq!(db.get(b"k0042").await.unwrap(), Some(Bytes::from("v42")));
}

/// Leaves and runs much larger than a scan page, so scans resume mid-leaf
/// with versions interleaved from several runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paged_scans_match_model() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.tree = TreeConfig {
        leaf_max_bytes: 16 << 10,
        internal_max_bytes: 32 << 10,
        max_fanout: 8,
        max_runs_per_child: 6,
        purge_tombstones_after: HOUR,
        ..TreeConfig::default()
    };
    cfg.checkpoint.bytes = 1 << 30;
    let db = open_with(&os, &clock, cfg).await;
    let mut model = Model::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    for round in 0..30u64 {
        for _ in 0..200 {
            clock.tick();
            let k = Bytes::from(format!("k{:05}", next(4000)));
            if next(5) == 0 {
                db.delete(k.clone()).await.unwrap();
                model.remove(&k);
            } else {
                let v = Bytes::from(format!("r{round}-{}", next(1000)));
                db.put(k.clone(), v.clone()).await.unwrap();
                model.insert(k, v);
            }
        }
        db.checkpoint().await.unwrap();
    }
    let shape = db.tree_shape().await.unwrap();
    assert!(shape.runs > 10, "runs must accumulate: {shape:?}");
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
    for _ in 0..200 {
        let a = next(4100);
        let len = next(400) as usize;
        let range = KeyRange::new(
            Bound::Included(Bytes::from(format!("k{a:05}"))),
            Bound::Unbounded,
        );
        let mut it = db.scan(range.clone());
        let mut got = Vec::new();
        while got.len() < len {
            match it.next().await.unwrap() {
                Some(kv) => got.push(kv),
                None => break,
            }
        }
        let want: Vec<_> = model_range(&model, &range).into_iter().take(len).collect();
        assert_eq!(got, want, "scan from k{a:05} len {len}");
    }
}

/// A reader follows the log past its manifest, across checkpoints and
/// writer changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readers_follow_the_log_and_new_manifests() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let r = reader(&os).await;
    let a = open(&os, &clock).await;
    clock.tick();
    a.put("k", "1").await.unwrap();
    assert_eq!(r.get("k").await.unwrap(), None, "not refreshed yet");
    assert_eq!(
        r.get_with("k", LATEST).await.unwrap().as_deref(),
        Some(&b"1"[..])
    );
    a.checkpoint().await.unwrap();
    clock.tick();
    a.put("k", "2").await.unwrap();
    drop(a);
    // Past the old writer's log, its replacement's fence, and on.
    let b = open(&os, &clock).await;
    clock.tick();
    b.put("j", "3").await.unwrap();
    r.refresh().await.unwrap();
    assert_eq!(r.get("k").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(r.get("j").await.unwrap().as_deref(), Some(&b"3"[..]));
    b.checkpoint().await.unwrap();
    r.refresh().await.unwrap();
    assert_eq!(r.manifest_id(), b.metrics().manifest_id);
    assert_eq!(r.get("j").await.unwrap().as_deref(), Some(&b"3"[..]));
}

/// Busy readers on cached data must not starve the log or checkpoints
/// (cache hits never suspend; reads yield via tokio's cooperative budget).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_readers_do_not_starve_writes() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Arc::new(
        Db::builder(ROOT, os.clone())
            .with_settings(tiny_config())
            .build()
            .await
            .unwrap(),
    );
    db.put("hot", "v").await.unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let (db, stop) = (db.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    db.get("hot").await.unwrap();
                }
            })
        })
        .collect();
    for i in 0..20 {
        tokio::time::timeout(Duration::from_secs(5), db.put(format!("k{i}"), "v"))
            .await
            .expect("a write starved")
            .unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    futures::future::join_all(readers).await;
    db.status().unwrap();
}

async fn wait_until(what: &str, f: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting: {what}"));
}

/// Copies every object except segments matching `skip`, like bucket
/// replication that hasn't caught up.
async fn replicate(
    from: &Arc<dyn ObjectStore>,
    to: &Arc<dyn ObjectStore>,
    skip: impl Fn(&str) -> bool,
) {
    let metas: Vec<_> = from
        .list(Some(&Path::from(ROOT)))
        .try_collect()
        .await
        .unwrap();
    for m in metas.into_iter().filter(|m| !skip(m.location.as_ref())) {
        let data = from.get(&m.location).await.unwrap().bytes().await.unwrap();
        to.put(&m.location, data.into()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_reader_falls_back_to_origin_for_lagging_segments() {
    let home: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let replica: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&home, &clock).await;
    let mut model = Model::new();
    for i in 0..200u32 {
        clock.tick();
        let (k, v) = (
            Bytes::from(format!("k{:03}", i % 150)),
            Bytes::from(format!("v{i}")),
        );
        db.put(k.clone(), v.clone()).await.unwrap();
        model.insert(k, v);
    }
    db.checkpoint().await.unwrap();
    db.close().await.unwrap();
    // Every segment is missing from the replica.
    replicate(&home, &replica, |p| p.contains("/seg/")).await;

    let cold = |origin: bool| {
        let b = DbReader::builder(ROOT, replica.clone()).with_settings(reader_config());
        match origin {
            true => b.with_origin(home.clone(), ROOT),
            false => b,
        }
        .build()
    };
    let without = cold(false).await.unwrap();
    assert!(matches!(
        without.scan(KeyRange::all()).collect().await,
        Err(Error::SnapshotExpired(_))
    ));
    let with = cold(true).await.unwrap();
    let all = with.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

/// A writer that opens with a clock behind its predecessor's still stamps
/// newer timestamps, even after the versions that carried the newest ones
/// are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lagging_new_writer_still_stamps_newer_timestamps() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    clock.tick();
    a.put("k", "old").await.unwrap();
    clock.tick();
    let mut batch = WriteBatch::new();
    batch.delete("k");
    let deleted_at = a.write(batch).await.unwrap().unwrap();
    a.checkpoint().await.unwrap();
    assert_eq!(a.tree_shape().await.unwrap().tombstones, 0);
    a.close().await.unwrap();

    let lagging = Db::builder(ROOT, os.clone())
        .with_settings(tiny_config())
        .with_clock(Arc::new(Skewed {
            base: clock.clone(),
            lag_ms: 60_000,
        }))
        .build()
        .await
        .unwrap();
    let mut batch = WriteBatch::new();
    batch.put("k", "late");
    let wrote_at = lagging.write(batch).await.unwrap().unwrap();
    assert!(wrote_at > deleted_at, "{wrote_at:?} <= {deleted_at:?}");
}

/// Faulty store with segment uploads held, so checkpoints can't finish.
async fn stuck_checkpoints(
    limit: usize,
) -> (
    Arc<faulty::Hold>,
    Arc<ManualClock>,
    Db,
    Arc<dyn ObjectStore>,
) {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let segments = faulty.hold_puts(|p| p.ends_with(".seg"), usize::MAX);
    let os: Arc<dyn ObjectStore> = faulty;
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.checkpoint.max_unapplied_bytes = limit;
    let db = open_with(&os, &clock, cfg).await;
    (segments, clock, db, os)
}

/// Backpressure counts what memory holds: every unapplied batch, even when
/// they all overwrite one key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_limit_counts_repeated_overwrites() {
    let (segments, clock, db, _) = stuck_checkpoints(2_000).await;
    let value = vec![1u8; 100];
    let mut written = 0u32;
    for _ in 0..200 {
        clock.tick();
        match tokio::time::timeout(Duration::from_millis(200), db.put("k", &value)).await {
            Ok(r) => {
                r.unwrap();
                written += 1;
            }
            Err(_) => break,
        }
    }
    // About 2,000 / 100 batches fit; one key's merged size never would stop it.
    assert!(
        written < 40,
        "{written} overwrites passed a 2,000 B backlog limit"
    );
    assert!(db.metrics().unapplied_bytes >= 2_000);
    segments.release();
    db.put("k", "after").await.unwrap();
}

/// `close()` writes out a queued write past backpressure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_flushes_queued_writes_past_backpressure() {
    let (segments, clock, db, os) = stuck_checkpoints(2_000).await;
    let value = vec![1u8; 100];
    let mut written = 0u32;
    while db.metrics().unapplied_bytes < 2_000 {
        clock.tick();
        db.put(written.to_be_bytes(), &value).await.unwrap();
        written += 1;
    }
    clock.tick();
    let mut batch = WriteBatch::new();
    batch.put("queued", "q");
    let queued = WriteOptions {
        durability: Durability::Queued,
    };
    db.write_with(batch, queued).await.unwrap();
    let closing = tokio::spawn(db.close());
    // The queued write goes out before any checkpoint frees room.
    wait_until("queued write durable", || {
        futures::executor::block_on(sidra::Admin::new(ROOT, os.clone()).log())
            // The writes, the queued one, and the writer's fence.
            .is_ok_and(|l| l.unapplied == written as usize + 2)
    })
    .await;
    segments.release();
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .expect("close hung")
        .unwrap()
        .unwrap();
    let db = open(&os, &clock).await;
    assert_eq!(db.get("queued").await.unwrap().as_deref(), Some(&b"q"[..]));
}
