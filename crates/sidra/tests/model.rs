//! Model-based tests: random op sequences over several concurrent writers
//! against a `BTreeMap`, with tiny nodes so splits, buffer flushes, removals,
//! relocation and root growth/shrink happen constantly.
//!
//! A manual clock, advanced before every write, makes timestamp order equal
//! call order, so the model is plain sequential last-writer-wins. Queued
//! (not yet durable) writes and checkpoints that flush only the applier make
//! late writes from slow writers happen for real.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use proptest::prelude::*;
use sidra::{
    ApplierRole, CacheConfig, CheckpointConfig, CleanerConfig, Clock, Db, DbReader, Durability,
    Error, ErrorKind, Gc, KeyRange, LeaseConfig, PollConfig, ReaderSettings, SegmentId, Settings,
    TreeConfig, Ts, WalConfig, Warm, WriteBatch, WriteOptions,
};

#[path = "support/faulty.rs"]
mod faulty;
use faulty::{Faults, Faulty};

const ROOT: &str = "db";
const WRITERS: usize = 3;
const HOUR: Duration = Duration::from_secs(3600);

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(1_000)))
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

/// A process clock running `lag_ms` behind the shared one.
struct Skewed {
    base: Arc<ManualClock>,
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
            max_unapplied_bytes: 1 << 30,
        },
        poll: PollConfig {
            interval: Duration::from_millis(5),
            max_interval: Duration::from_millis(20),
            discovery_interval: Duration::from_millis(20),
            follow: sidra::Follow::AllWriters,
        },
        lease: LeaseConfig {
            interval: Duration::from_millis(5),
            ttl: HOUR,
        },
        applier: ApplierRole::Auto { steal_after: HOUR },
        gc: Gc::Off,
        // Clean eagerly so relocation runs constantly.
        cleaner: CleanerConfig {
            clean_below_live: 0.6,
            clean_inner_below_live: 0.6,
            max_moved_bytes: 2048,
        },
        // Exercise the compressed path everywhere in tests.
        compression: sidra::Compression::Lz4,
        log: sidra::LogHome::Home,
        regions: BTreeMap::new(),
        warm: Warm::Blocking,
        rewarm: Warm::Blocking,
    }
}

fn reader_config() -> ReaderSettings {
    ReaderSettings {
        cache: CacheConfig {
            inner_bytes: 4096,
            leaf_bytes: 1024,
        },
        refresh_interval: None,
        discovery_interval: Duration::from_millis(20),
        warm: Warm::Off,
        rewarm: Warm::Off,
        regions: BTreeMap::new(),
        origin: None,
    }
}

async fn open(os: &Arc<dyn ObjectStore>, clock: &Arc<ManualClock>) -> Db {
    Db::builder(ROOT, os.clone())
        .with_settings(tiny_config())
        .with_clock(clock.clone())
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
    Put(usize, u8, Vec<u8>),
    PutQueued(usize, u8, Vec<u8>),
    Delete(usize, u8),
    Get(usize, u8),
    Scan(usize, u8, u8),
    Checkpoint,
    Claim(usize),
    /// A fresh process replaces the writer, with its clock this many ms
    /// behind.
    Reopen(usize, u64),
    /// The writer dies without closing; an applier dies this many
    /// microseconds into a checkpoint. Its replacement's clock lags by the
    /// second value (ms).
    Crash(usize, u64, u64),
    ReaderScan,
}

fn key(k: u8) -> Bytes {
    Bytes::from(format!("key{k:03}"))
}

fn op() -> impl Strategy<Value = Op> {
    let w = 0..WRITERS;
    let value = proptest::collection::vec(any::<u8>(), 0..40);
    prop_oneof![
        6 => (w.clone(), 0u8..60, value.clone()).prop_map(|(w, k, v)| Op::Put(w, k, v)),
        3 => (w.clone(), 0u8..60, value).prop_map(|(w, k, v)| Op::PutQueued(w, k, v)),
        4 => (w.clone(), 0u8..60).prop_map(|(w, k)| Op::Delete(w, k)),
        2 => (w.clone(), 0u8..60).prop_map(|(w, k)| Op::Get(w, k)),
        1 => (w.clone(), 0u8..60, 0u8..60).prop_map(|(w, a, b)| Op::Scan(w, a, b)),
        2 => Just(Op::Checkpoint),
        1 => w.clone().prop_map(Op::Claim),
        1 => (w.clone(), 0u64..300).prop_map(|(w, lag)| Op::Reopen(w, lag)),
        1 => (w, 0u64..3000, 0u64..300).prop_map(|(w, us, lag)| Op::Crash(w, us, lag)),
        1 => Just(Op::ReaderScan),
    ]
}

type Model = BTreeMap<Bytes, Bytes>;

/// Last-writer-wins by timestamp: what every reader must see once all
/// acknowledged writes are visible, whatever the writers' clock skew.
#[derive(Default)]
struct Lww(BTreeMap<Bytes, (Ts, Option<Bytes>)>);

impl Lww {
    fn apply(&mut self, k: Bytes, ts: Ts, v: Option<Bytes>) {
        match self.0.get(&k) {
            Some((old, _)) if *old >= ts => {}
            _ => {
                self.0.insert(k, (ts, v));
            }
        }
    }

    fn visible(&self) -> Model {
        self.0
            .iter()
            .filter_map(|(k, (_, v))| v.clone().map(|v| (k.clone(), v)))
            .collect()
    }
}

/// Per-writer clock lags: often none (exact order), else up to 300 ms, far
/// more than the gaps between writes, so writers' timestamps interleave.
fn lags() -> impl Strategy<Value = Vec<u64>> {
    prop_oneof![
        1 => Just(vec![0; WRITERS]),
        3 => proptest::collection::vec(0u64..300, WRITERS),
    ]
}

fn model_range(model: &Model, range: &KeyRange) -> Vec<(Bytes, Bytes)> {
    model
        .range((range.start.clone(), range.end.clone()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Makes every writer's queued writes durable and lets `reader` see them.
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
            Err(e) if e.is_retryable() => last = Some(e),
            Err(e) => panic!("{what}: {e}"),
        }
    }
    panic!("{what}: still failing after 100 attempts: {last:?}");
}

/// Several writers on one store, optionally behind injected faults.
struct World {
    os: Arc<dyn ObjectStore>,
    clock: Arc<ManualClock>,
    /// How far behind the shared clock each writer slot's clock runs.
    lags: Vec<u64>,
    dbs: Vec<Db>,
    faulty: Option<Arc<Faulty>>,
}

impl World {
    async fn new(faults: Option<(Faults, u64)>, lags: Vec<u64>) -> Self {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let faulty = faults.map(|(f, seed)| Arc::new(Faulty::new(mem, f, seed)));
        let os: Arc<dyn ObjectStore> = match &faulty {
            Some(f) => f.clone(),
            None => Arc::new(InMemory::new()),
        };
        let clock = ManualClock::new();
        let mut w = Self {
            os,
            clock,
            lags,
            dbs: Vec::new(),
            faulty,
        };
        for slot in 0..WRITERS {
            let db = w.open(slot).await;
            w.dbs.push(db);
        }
        w
    }

    async fn open(&self, slot: usize) -> Db {
        let clock = Arc::new(Skewed {
            base: self.clock.clone(),
            lag_ms: self.lags[slot],
        });
        until_ok("open", || {
            Db::builder(ROOT, self.os.clone())
                .with_settings(tiny_config())
                .with_clock(clock.clone())
                .build()
        })
        .await
    }

    /// Replaces writer `w` with a fresh one.
    async fn reopen(&mut self, w: usize) {
        let fresh = self.open(w).await;
        let old = std::mem::replace(&mut self.dbs[w], fresh);
        match (old.close().await, &self.faulty) {
            (Ok(()), _) => {}
            // A lost writer, or a store failure while releasing the role.
            (Err(e), Some(_)) if e.is_retryable() || e.kind() == ErrorKind::WriterLost => {}
            (Err(e), _) => panic!("close: {e}"),
        }
    }

    /// Writes until acknowledged, replacing the writer if it's lost (then
    /// the write may have landed already; repeating it is harmless).
    async fn write(&mut self, w: usize, batch: WriteBatch, opts: WriteOptions) -> Ts {
        for _ in 0..100 {
            match self.dbs[w].write_with(batch.clone(), opts).await {
                Ok(ts) => return ts.expect("non-empty batch"),
                Err(e) if e.is_retryable() => {}
                Err(e) if e.kind() == ErrorKind::WriterLost => self.reopen(w).await,
                Err(e) => panic!("write: {e}"),
            }
        }
        panic!("write: still failing");
    }

    /// Kills writer `w` (an applier part-way through a checkpoint) and
    /// starts a fresh one. Queued writes are flushed first: only
    /// acknowledged writes must survive a crash.
    async fn crash(&mut self, w: usize, after: Duration) {
        match self.dbs[w].flush_wal().await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::WriterLost => {}
            Err(e) => panic!("flush: {e}"),
        }
        if self.dbs[w].is_applier() {
            // Cancelling the checkpoint stops it at whatever await it
            // reached: segments may be uploaded with no manifest.
            let _ = tokio::time::timeout(after, self.dbs[w].checkpoint()).await;
        }
        let fresh = self.open(w).await;
        drop(std::mem::replace(&mut self.dbs[w], fresh));
    }

    /// Makes every durable write visible to writer `reader`.
    async fn sync(&mut self, reader: usize) {
        for w in 0..self.dbs.len() {
            match self.dbs[w].flush_wal().await {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::WriterLost => self.reopen(w).await,
                Err(e) => panic!("flush: {e}"),
            }
        }
        until_ok("refresh", || self.dbs[reader].refresh()).await;
    }
}

/// Runs a checkpoint on whoever holds the applier role, claiming it with
/// writer 0 if nobody does. A store failure just means no checkpoint.
async fn checkpoint(dbs: &[Db]) {
    // Released roles are taken by background claims too, so the claim can
    // lose a race; then whoever won is the applier.
    let applier = loop {
        match dbs.iter().position(Db::is_applier) {
            Some(i) => break i,
            None => match dbs[0].claim_applier().await {
                Ok(true) => break 0,
                Ok(false) => tokio::task::yield_now().await,
                Err(e) if e.is_retryable() => return,
                Err(e) => panic!("claim: {e}"),
            },
        }
    };
    match dbs[applier].checkpoint().await {
        Ok(()) => assert_accounting(&dbs[applier]).await,
        // Lost the role to a concurrent claim, or the store failed: fine.
        Err(Error::NotApplier) => {}
        Err(e) if e.is_retryable() => {}
        Err(e) => panic!("checkpoint: {e}"),
    }
}

async fn run(ops: Vec<Op>, lags: Vec<u64>, faults: Option<(Faults, u64)>) {
    let mut world = World::new(faults, lags).await;
    let mut lww = Lww::default();
    for op in ops {
        match op {
            Op::Put(w, k, v) => {
                world.clock.tick();
                let mut batch = WriteBatch::new();
                batch.put(key(k), &v);
                let ts = world.write(w, batch, WriteOptions::default()).await;
                lww.apply(key(k), ts, Some(Bytes::from(v)));
            }
            Op::PutQueued(w, k, v) => {
                world.clock.tick();
                let mut batch = WriteBatch::new();
                batch.put(key(k), &v);
                // A queued write is lost if its writer is lost before the
                // flush, so under faults only acknowledged writes count.
                let durability = match world.faulty {
                    Some(_) => Durability::AwaitDurable,
                    None => Durability::Queued,
                };
                let ts = world.write(w, batch, WriteOptions { durability }).await;
                lww.apply(key(k), ts, Some(Bytes::from(v)));
            }
            Op::Delete(w, k) => {
                world.clock.tick();
                let mut batch = WriteBatch::new();
                batch.delete(key(k));
                let ts = world.write(w, batch, WriteOptions::default()).await;
                lww.apply(key(k), ts, None);
            }
            Op::Get(w, k) => {
                world.sync(w).await;
                let model = lww.visible();
                let got = until_ok("get", || world.dbs[w].get(key(k))).await;
                assert_eq!(got, model.get(&key(k)).cloned(), "get {k} via writer {w}");
            }
            Op::Scan(w, a, b) => {
                world.sync(w).await;
                let model = lww.visible();
                let (lo, hi) = (a.min(b), a.max(b));
                let ranges = [
                    KeyRange::new(Bound::Included(key(lo)), Bound::Excluded(key(hi))),
                    KeyRange::new(Bound::Excluded(key(lo)), Bound::Included(key(hi))),
                    KeyRange::new(Bound::Unbounded, Bound::Included(key(hi))),
                    KeyRange::new(Bound::Included(key(lo)), Bound::Unbounded),
                ];
                for r in ranges {
                    let got = until_ok("scan", || world.dbs[w].scan(r.clone()).collect()).await;
                    assert_eq!(got, model_range(&model, &r), "scan {r:?} via writer {w}");
                }
            }
            Op::Checkpoint => checkpoint(&world.dbs).await,
            Op::Claim(w) => match world.dbs[w].claim_applier().await {
                Ok(_) => {}
                Err(e) if e.is_retryable() => {}
                Err(e) => panic!("claim: {e}"),
            },
            Op::Reopen(w, lag) => {
                world.lags[w] = lag;
                world.reopen(w).await
            }
            Op::Crash(w, us, lag) => {
                world.lags[w] = lag;
                world.crash(w, Duration::from_micros(us)).await
            }
            Op::ReaderScan => {
                world.sync(0).await;
                let model = lww.visible();
                let reader = until_ok("reader open", || {
                    DbReader::builder(ROOT, world.os.clone())
                        .with_settings(reader_config())
                        .build()
                })
                .await;
                let got = until_ok("reader scan", || reader.scan(KeyRange::all()).collect()).await;
                assert_eq!(got, model_range(&model, &KeyRange::all()), "reader scan");
            }
        }
    }
    // Verify without faults: everything acknowledged survived them.
    if let Some(f) = &world.faulty {
        f.set_enabled(false);
    }
    world.sync(1).await;
    let model = lww.visible();
    let all = world.dbs[1].scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
    checkpoint(&world.dbs).await;
    let World { os, clock, dbs, .. } = world;
    for db in dbs {
        db.close().await.unwrap();
    }
    let db = open(&os, &clock).await;
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()), "after reopen");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]
    #[test]
    fn matches_model(ops in proptest::collection::vec(op(), 1..250), lags in lags()) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(run(ops, lags, None));
    }
}

proptest! {
    // Each retried fault sleeps for the backoff, so cases are slow; raise
    // with PROPTEST_CASES for stress runs.
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]
    /// Same, with store requests failing before and after they apply, and
    /// random delays.
    #[test]
    fn matches_model_under_faults(ops in proptest::collection::vec(op(), 1..150), lags in lags(), seed in any::<u64>()) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let faults = Faults {
            fail_before: 0.05,
            fail_after: 0.05,
            max_delay: Duration::from_millis(1),
        };
        rt.block_on(run(ops, lags, Some((faults, seed))));
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_all_land() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let dbs: Vec<Arc<Db>> = futures::future::join_all((0..4).map(|_| open(&os, &clock)))
        .await
        .into_iter()
        .map(Arc::new)
        .collect();
    assert_eq!(dbs.iter().filter(|d| d.is_applier()).count(), 1);
    let tasks: Vec<_> = dbs
        .iter()
        .enumerate()
        .map(|(w, db)| {
            let db = db.clone();
            tokio::spawn(async move {
                for i in 0..200u32 {
                    db.put(format!("w{w}-{i:04}"), format!("{w}:{i}"))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    futures::future::try_join_all(tasks).await.unwrap();
    let applier = dbs.iter().find(|d| d.is_applier()).unwrap();
    applier.checkpoint().await.unwrap();
    let reader = DbReader::builder(ROOT, os.clone())
        .with_settings(reader_config())
        .build()
        .await
        .unwrap();
    let all = reader.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all.len(), 800);
    assert_eq!(
        reader.get(b"w3-0199").await.unwrap(),
        Some(Bytes::from("3:199"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applier_failover_applies_the_dead_appliers_writes() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.applier = ApplierRole::Auto {
        steal_after: Duration::from_secs(10),
    };
    cfg.checkpoint.bytes = 1 << 30;
    let a = Db::builder(ROOT, os.clone())
        .with_settings(cfg.clone())
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    let b = Db::builder(ROOT, os.clone())
        .with_settings(cfg)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    assert!(a.is_applier() && !b.is_applier());
    clock.tick();
    a.put("from-a", "1").await.unwrap();
    clock.tick();
    b.put("from-b", "2").await.unwrap();
    // `a` dies without a final checkpoint.
    drop(a);
    clock.advance(Duration::from_secs(60));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !b.is_applier() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("b takes over the applier role");
    b.checkpoint().await.unwrap();
    let reader = DbReader::builder(ROOT, os)
        .with_settings(reader_config())
        .build()
        .await
        .unwrap();
    assert_eq!(reader.get(b"from-a").await.unwrap(), Some(Bytes::from("1")));
    assert_eq!(reader.get(b"from-b").await.unwrap(), Some(Bytes::from("2")));
    assert!(b.metrics().unapplied_bytes == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_applier_is_fenced_but_keeps_writing() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    let b = open(&os, &clock).await;
    assert!(a.is_applier());
    assert!(b.claim_applier().await.unwrap());
    clock.tick();
    a.put("x", "1").await.unwrap();
    assert!(matches!(a.checkpoint().await, Err(Error::NotApplier)));
    assert!(!a.is_applier());
    b.checkpoint().await.unwrap();
    b.refresh().await.unwrap();
    assert_eq!(b.get(b"x").await.unwrap(), Some(Bytes::from("1")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstones_are_dropped_once_safe() {
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
    // Push everything down with more checkpoints as floors move past the
    // deletes.
    for round in 0..20u32 {
        clock.advance(Duration::from_secs(1));
        clock.tick();
        db.put(format!("z{round:04}"), "v").await.unwrap();
        db.checkpoint().await.unwrap();
    }
    let shape = db.tree_shape().await.unwrap();
    assert_eq!(shape.tombstones, 0, "{shape:?}");
    assert_eq!(db.scan(KeyRange::all()).collect().await.unwrap().len(), 20);
}

async fn segment_objects(os: &Arc<dyn ObjectStore>) -> usize {
    let prefix = Path::from(format!("{ROOT}/seg"));
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
    let before = segment_objects(&os).await;

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
    let after = segment_objects(&os).await;
    assert!(after < before);
    assert_eq!(
        after,
        db.segment_table().len(),
        "only referenced segments remain"
    );
    // The cleaner keeps space amplification bounded.
    let s = db.metrics();
    assert!(s.live_bytes * 10 >= s.segment_bytes * 4, "{s:?}");

    db.close().await.unwrap();
    let reader = DbReader::builder(ROOT, os)
        .with_settings(reader_config())
        .build()
        .await
        .unwrap();
    let all = reader.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_segments_are_deleted_on_claim() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let db = open(&os, &clock).await;
    for i in 0..200u32 {
        clock.tick();
        db.put(format!("k{i:04}"), format!("v{i}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    db.close().await.unwrap();
    let published = segment_objects(&os).await;
    // An applier that uploaded a segment but died before publishing.
    let orphan = Path::from(format!("{ROOT}/seg/{:020}-{:020}.seg", 1, 1_000_000));
    os.put(&orphan, "junk".into()).await.unwrap();
    let db = open(&os, &clock).await;
    assert!(db.claim_applier().await.unwrap());
    assert_eq!(segment_objects(&os).await, published);
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
    };
    cfg.checkpoint.bytes = 1 << 30;
    let db = Db::builder(ROOT, os.clone())
        .with_settings(cfg)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_only_writers_see_others_after_apply() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let applier = open(&os, &clock).await;
    let mut cfg = tiny_config();
    cfg.poll.follow = sidra::Follow::ManifestOnly;
    cfg.applier = ApplierRole::Never;
    let quiet = Db::builder(ROOT, os.clone())
        .with_settings(cfg)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    clock.tick();
    applier.put("k", "v").await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        quiet.get("k").await.unwrap(),
        None,
        "not pulled before apply"
    );
    let fresh = sidra::ReadOptions {
        freshness: sidra::Freshness::Latest,
    };
    assert_eq!(
        quiet.get_with("k", fresh).await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
    clock.tick();
    applier.put("k2", "v2").await.unwrap();
    applier.checkpoint().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while quiet.get("k2").await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("visible once applied");
    let m = quiet.metrics();
    assert!(!m.is_applier && m.requests.gets > 0 && m.cache.hits + m.cache.misses > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_applier_hands_over_and_dead_lease_triggers_takeover() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.checkpoint.interval = Duration::from_secs(1);
    cfg.lease.ttl = Duration::from_secs(5);
    let build = |cfg: Settings| {
        Db::builder(ROOT, os.clone())
            .with_settings(cfg)
            .with_clock(clock.clone())
            .build()
    };
    // Graceful: a closing applier releases, the next writer with work takes over.
    let a = build(cfg.clone()).await.unwrap();
    assert!(a.is_applier());
    a.close().await.unwrap();
    let b = build(cfg.clone()).await.unwrap();
    clock.tick();
    b.put("k", "v").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !b.is_applier() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("takes the released role");
    // Crash: b dies; once its lease expires, c takes over without waiting
    // the full steal_after.
    let c = build(cfg).await.unwrap();
    drop(b);
    clock.tick();
    c.put("k2", "v2").await.unwrap();
    clock.advance(Duration::from_secs(6));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !c.is_applier() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("takes over from a dead applier");
    c.checkpoint().await.unwrap();
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

/// Busy readers on cached data must not starve lease renewal (cache hits
/// never suspend; reads yield via tokio's cooperative budget). Without the
/// yield this test hangs: the starved runtime never runs the put loop either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_readers_do_not_starve_the_lease() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut cfg = tiny_config();
    cfg.lease = LeaseConfig {
        interval: Duration::from_millis(20),
        ttl: Duration::from_millis(300),
    };
    let db = Arc::new(
        Db::builder(ROOT, os.clone())
            .with_settings(cfg)
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
        db.put(format!("k{i}"), "v").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Relaxed);
    futures::future::join_all(readers).await;
    db.status().unwrap();
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
async fn regional_logs_are_followed_applied_and_collected() {
    let home: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let eu_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let eu = sidra::Region::new("eu");
    let clock = ManualClock::new();
    let local = open(&home, &clock).await;
    let remote = Db::builder(ROOT, home.clone())
        .with_settings(Settings {
            log: sidra::LogHome::Region(eu.clone()),
            applier: ApplierRole::Never,
            ..tiny_config()
        })
        .with_region(eu.clone(), eu_store.clone(), ROOT)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    clock.tick();
    remote.put("from-eu", "1").await.unwrap();
    clock.tick();
    local.put("from-home", "2").await.unwrap();

    // The remote writer's log is in its own store, not the home one.
    let wid = remote.writer_id();
    assert!(objects(&eu_store, &format!("wal/{wid}")).await > 0);
    assert_eq!(objects(&home, &format!("wal/{wid}")).await, 0);

    // The applier needs the region to pull it.
    let blind = Db::builder(ROOT, home.clone())
        .with_settings(Settings {
            applier: ApplierRole::Never,
            ..tiny_config()
        })
        .with_clock(clock.clone())
        .build()
        .await;
    assert!(
        matches!(blind, Err(Error::InvalidConfig(_))),
        "{:?}",
        blind.err()
    );

    // The home writer was opened without the region: give the role to one
    // that has it.
    local.close().await.unwrap();
    let applier = Db::builder(ROOT, home.clone())
        .with_settings(tiny_config())
        .with_region(eu.clone(), eu_store.clone(), ROOT)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    assert!(applier.claim_applier().await.unwrap() || applier.is_applier());
    applier.refresh().await.unwrap();
    assert_eq!(
        applier.get("from-eu").await.unwrap().as_deref(),
        Some(&b"1"[..])
    );
    applier.checkpoint().await.unwrap();

    let reader = DbReader::builder(ROOT, home.clone())
        .with_settings(reader_config())
        .with_region(eu.clone(), eu_store.clone(), ROOT)
        .build()
        .await
        .unwrap();
    assert_eq!(
        reader.get("from-eu").await.unwrap().as_deref(),
        Some(&b"1"[..])
    );
    assert_eq!(
        reader.get("from-home").await.unwrap().as_deref(),
        Some(&b"2"[..])
    );

    // GC deletes applied log objects from the regional store too.
    clock.tick();
    applier.checkpoint().await.unwrap();
    clock.advance(Duration::from_secs(120));
    let stats = applier.run_gc(Duration::from_secs(60)).await.unwrap();
    assert!(stats.wals > 0, "{stats:?}");
    assert_eq!(objects(&eu_store, &format!("wal/{wid}")).await, 0);
    remote.close().await.unwrap();
    applier.close().await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standby_applies_only_while_no_primary_can() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.checkpoint.interval = Duration::from_secs(1);
    cfg.lease.ttl = Duration::from_secs(5);
    let standby_cfg = Settings {
        applier: ApplierRole::Standby {
            after: Duration::from_secs(30),
        },
        ..cfg.clone()
    };
    let build = |cfg: Settings| {
        Db::builder(ROOT, os.clone())
            .with_settings(cfg)
            .with_clock(clock.clone())
            .build()
    };
    let p = build(cfg.clone()).await.unwrap();
    assert!(p.is_applier());
    let s = build(standby_cfg).await.unwrap();
    clock.tick();
    s.put("a", "1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!s.is_applier(), "a live primary keeps the role");

    // The primary crashes: the standby takes over after `after`.
    drop(p);
    clock.tick();
    s.put("b", "2").await.unwrap();
    clock.advance(Duration::from_secs(31));
    wait_until("standby takes over", || s.is_applier()).await;
    s.checkpoint().await.unwrap();

    // A primary returns: the standby hands the role back at its next
    // checkpoint, and the primary takes it.
    let q = build(cfg).await.unwrap();
    assert!(!q.is_applier());
    clock.tick();
    s.put("c", "3").await.unwrap();
    s.checkpoint().await.unwrap();
    assert!(!s.is_applier(), "hands back");
    clock.tick();
    q.put("d", "4").await.unwrap();
    wait_until("primary takes the released role", || q.is_applier()).await;
    q.checkpoint().await.unwrap();
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        assert_eq!(
            q.get(k).await.unwrap().as_deref(),
            Some(v.as_bytes()),
            "{k}"
        );
    }
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

/// A writer that opens after a tombstone was dropped, with a clock behind
/// the applier's, must not write a version older than that tombstone:
/// otherwise its write shows although the delete is newer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_new_writer_cannot_undercut_a_dropped_tombstone() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    clock.advance(Duration::from_secs(100));
    let a = open(&os, &clock).await;
    clock.tick();
    a.put("k", "old").await.unwrap();
    clock.tick();
    let deleted_at = a
        .write({
            let mut b = WriteBatch::new();
            b.delete("k");
            b
        })
        .await
        .unwrap()
        .unwrap();
    // Checkpoint until the tombstone is gone.
    for round in 0..10u32 {
        clock.advance(Duration::from_secs(1));
        clock.tick();
        a.put(format!("z{round}"), "v").await.unwrap();
        a.checkpoint().await.unwrap();
    }
    assert_eq!(a.tree_shape().await.unwrap().tombstones, 0);

    let lagging = Db::builder(ROOT, os.clone())
        .with_settings(tiny_config())
        .with_clock(Arc::new(Skewed {
            base: clock.clone(),
            // Behind by more than everything that happened since the delete.
            lag_ms: 60_000,
        }))
        .build()
        .await
        .unwrap();
    let mut batch = WriteBatch::new();
    batch.put("k", "late");
    let wrote_at = lagging.write(batch).await.unwrap().unwrap();
    assert!(wrote_at > deleted_at, "{wrote_at:?} <= {deleted_at:?}");
    lagging.flush_wal().await.unwrap();
    a.refresh().await.unwrap();
    // The later write wins, consistently before and after applying.
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some(&b"late"[..]));
    a.checkpoint().await.unwrap();
    assert_eq!(a.get("k").await.unwrap().as_deref(), Some(&b"late"[..]));
}

/// Two writers on a store whose PUTs can be held in flight: `b` the applier,
/// `a` a plain writer, both on one clock (so nothing here is clock skew).
async fn held_pair() -> (Arc<Faulty>, Arc<ManualClock>, Db, Db) {
    let faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), Faults::NONE, 0));
    let os: Arc<dyn ObjectStore> = faulty.clone();
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.lease.ttl = Duration::from_secs(5);
    let build = |cfg: Settings| {
        Db::builder(ROOT, os.clone())
            .with_settings(cfg)
            .with_clock(clock.clone())
            .build()
    };
    let b = build(cfg.clone()).await.unwrap();
    let a = build(Settings {
        applier: ApplierRole::Never,
        ..cfg
    })
    .await
    .unwrap();
    assert!(b.is_applier());
    (faulty, clock, a, b)
}

/// Lets unheld leases renew after the clock jumped (a lease interval is 5 ms).
async fn renewed() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn is_lease_of(w: sidra::WriterId) -> impl Fn(&str) -> bool + Send + Sync + 'static {
    let w = w.to_string();
    move |p| p.contains(&w) && p.ends_with("/lease")
}

fn is_log_of(w: sidra::WriterId) -> impl Fn(&str) -> bool + Send + Sync + 'static {
    let w = w.to_string();
    move |p| p.contains(&w) && p.ends_with(".wal")
}

/// A log write held in flight while its writer's lease runs out must not
/// land once the applier stopped counting that writer: a newer tombstone for
/// the key may already be gone, and the late write would resurrect the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_log_write_cannot_resurrect_a_dropped_tombstone() {
    let (faulty, clock, a, b) = held_pair().await;
    let leases = faulty.hold_puts(is_lease_of(a.writer_id()), usize::MAX);
    let log = faulty.hold_puts(is_log_of(a.writer_id()), 1);
    clock.tick();
    let a = Arc::new(a);
    let pending = tokio::spawn({
        let a = a.clone();
        async move { a.put("k", "old").await }
    });
    wait_until("log write in flight", || log.arrived() == 1).await;
    clock.tick();
    b.delete("k").await.unwrap();
    // `a`'s lease runs out; checkpoint until the tombstone is gone.
    clock.advance(Duration::from_secs(10));
    for round in 0..5u32 {
        clock.advance(Duration::from_secs(1));
        renewed().await;
        clock.tick();
        b.put(format!("z{round}"), "v").await.unwrap();
        b.checkpoint().await.unwrap();
    }
    assert_eq!(b.tree_shape().await.unwrap().tombstones, 0);

    log.release();
    let acked = pending.await.unwrap();
    let fresh = sidra::ReadOptions {
        freshness: sidra::Freshness::Latest,
    };
    let got = b.get_with("k", fresh).await.unwrap();
    assert_eq!(
        got, None,
        "deleted key resurrected; the late write returned {acked:?}"
    );
    assert_eq!(acked.unwrap_err().kind(), ErrorKind::WriterLost);
    leases.release();
}

/// A writer whose lease lapsed long enough for GC to forget it must not
/// acknowledge writes on its return: fresh readers would never see them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forgotten_writer_cannot_acknowledge_unreadable_writes() {
    let (faulty, clock, a, b) = held_pair().await;
    clock.tick();
    a.put("first", "1").await.unwrap();
    b.checkpoint().await.unwrap();
    let leases = faulty.hold_puts(is_lease_of(a.writer_id()), usize::MAX);
    // `a`'s lease runs out, then everything ages past the GC retention.
    for step in [10, 120, 120] {
        clock.advance(Duration::from_secs(step));
        renewed().await;
        clock.tick();
        b.put(format!("z{step}"), "v").await.unwrap();
        b.checkpoint().await.unwrap();
    }
    clock.advance(Duration::from_secs(120));
    renewed().await;
    let stats = b.run_gc(Duration::from_secs(60)).await.unwrap();
    assert!(stats.writers == 1 && stats.wals > 0, "{stats:?}");
    renewed().await;
    clock.tick();
    b.put("z", "v").await.unwrap();
    b.checkpoint().await.unwrap();

    // Renewal resumes under the same writer id.
    leases.release();
    renewed().await;
    clock.tick();
    let acked = a.put("second", "2").await;
    let reader = DbReader::builder(ROOT, faulty.clone() as Arc<dyn ObjectStore>)
        .with_settings(reader_config())
        .build()
        .await
        .unwrap();
    let seen = reader.get("second").await.unwrap();
    match acked {
        Ok(()) => assert_eq!(
            seen.as_deref(),
            Some(&b"2"[..]),
            "acknowledged but unreadable"
        ),
        Err(e) => assert_eq!(e.kind(), ErrorKind::WriterLost, "{e}"),
    }
}

/// A closing writer seals its own log. Once applied and past retention, GC
/// forgets the writer but keeps the seal, and its writes stay readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_writer_is_sealed_then_forgotten() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let a = open(&os, &clock).await;
    let c = open(&os, &clock).await;
    let cid = c.writer_id();
    clock.tick();
    c.put("x", "1").await.unwrap();
    c.close().await.unwrap();
    a.checkpoint().await.unwrap();
    let admin = sidra::Admin::new(ROOT, os.clone());
    let info = |ws: Vec<sidra::WriterInfo>| ws.into_iter().find(|w| w.id == cid);
    let sealed = info(admin.writers().await.unwrap()).unwrap();
    assert!(sealed.sealed && sealed.unapplied == 0, "{sealed:?}");

    for _ in 0..2 {
        clock.advance(Duration::from_secs(120));
        clock.tick();
        a.put("z", "v").await.unwrap();
        a.checkpoint().await.unwrap();
    }
    let stats = a.run_gc(Duration::from_secs(60)).await.unwrap();
    assert_eq!(stats.writers, 1, "{stats:?}");
    clock.tick();
    a.put("z", "w").await.unwrap();
    a.checkpoint().await.unwrap();
    assert!(info(admin.writers().await.unwrap()).is_none());
    assert_eq!(
        objects(&os, &format!("wal/{cid}")).await,
        1,
        "the seal stays"
    );
    let reader = DbReader::builder(ROOT, os.clone())
        .with_settings(reader_config())
        .build()
        .await
        .unwrap();
    assert_eq!(reader.get("x").await.unwrap().as_deref(), Some(&b"1"[..]));
}

/// Backpressure counts what memory holds: every unapplied batch, even when
/// they all overwrite one key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_limit_counts_repeated_overwrites() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut cfg = tiny_config();
    cfg.applier = ApplierRole::Never;
    cfg.checkpoint.max_unapplied_bytes = 2_000;
    let db = Db::builder(ROOT, os.clone())
        .with_settings(cfg)
        .with_clock(clock.clone())
        .build()
        .await
        .unwrap();
    let value = vec![1u8; 100];
    let mut written = 0;
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
}

/// A regional log store going down stalls nothing else: other writers'
/// writes still get applied, and no tombstone is dropped meanwhile (the
/// unreachable log might hold an older version of its key).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regional_outage_does_not_stall_other_writers() {
    let home: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let outage = Faults {
        fail_before: 1.0,
        fail_after: 0.0,
        max_delay: Duration::ZERO,
    };
    let eu_faulty = Arc::new(Faulty::new(Arc::new(InMemory::new()), outage, 1));
    eu_faulty.set_enabled(false);
    let eu_store: Arc<dyn ObjectStore> = eu_faulty.clone();
    let eu = sidra::Region::new("eu");
    let clock = ManualClock::new();
    let build = |settings: Settings| {
        Db::builder(ROOT, home.clone())
            .with_settings(settings)
            .with_region(eu.clone(), eu_store.clone(), ROOT)
            .with_clock(clock.clone())
            .build()
    };
    let applier = build(tiny_config()).await.unwrap();
    let remote = build(Settings {
        log: sidra::LogHome::Region(eu.clone()),
        applier: ApplierRole::Never,
        ..tiny_config()
    })
    .await
    .unwrap();
    clock.tick();
    remote.put("from-eu", "1").await.unwrap();
    clock.tick();
    applier.put("doomed", "x").await.unwrap();
    applier.checkpoint().await.unwrap();

    eu_faulty.set_enabled(true);
    clock.tick();
    applier.delete("doomed").await.unwrap();
    clock.tick();
    applier.put("from-home", "2").await.unwrap();
    for _ in 0..3 {
        clock.advance(Duration::from_secs(1));
        applier.checkpoint().await.unwrap();
    }
    let m = applier.metrics();
    assert_eq!(
        m.unapplied_bytes, 0,
        "home writes applied despite the outage"
    );
    assert_eq!(
        applier.get("from-home").await.unwrap().as_deref(),
        Some(&b"2"[..])
    );
    assert!(
        applier.tree_shape().await.unwrap().tombstones > 0,
        "tombstone kept while a log is unreachable"
    );
    // Reads that promise every write say so instead of missing some.
    assert!(applier.refresh().await.unwrap_err().is_retryable());

    eu_faulty.set_enabled(false);
    applier.refresh().await.unwrap();
    assert_eq!(
        applier.get("from-eu").await.unwrap().as_deref(),
        Some(&b"1"[..])
    );
    remote.close().await.unwrap();
    applier.close().await.unwrap();
}
