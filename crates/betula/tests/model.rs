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

use betula::{
    ApplierRole, CacheConfig, CheckpointConfig, CleanerConfig, Clock, Db, DbReader, Durability,
    Error, Gc, KeyRange, LeaseConfig, PollConfig, ReaderSettings, SegmentId, Settings, TreeConfig,
    WalConfig, Warm, WriteBatch, WriteOptions,
};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use proptest::prelude::*;

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

fn tiny_config() -> Settings {
    Settings {
        tree: TreeConfig {
            leaf_max_bytes: 200,
            internal_max_bytes: 400,
            max_fanout: 4,
            max_runs_per_child: 2,
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
            max_moved_bytes: 2048,
        },
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
    let reachable = db.tree_shape().await.unwrap().nodes.iter().fold(
        BTreeMap::<SegmentId, u64>::new(),
        |mut m, p| {
            *m.entry(p.seg).or_default() += u64::from(p.len);
            m
        },
    );
    let table: BTreeMap<SegmentId, u64> = db
        .segment_table()
        .iter()
        .map(|(id, u)| (*id, u.live))
        .collect();
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
    Reopen(usize),
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
        1 => w.prop_map(Op::Reopen),
        1 => Just(Op::ReaderScan),
    ]
}

type Model = BTreeMap<Bytes, Bytes>;

fn model_range(model: &Model, range: &KeyRange) -> Vec<(Bytes, Bytes)> {
    model
        .range((range.start.clone(), range.end.clone()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Makes every writer's queued writes durable and lets `reader` see them.
async fn sync(dbs: &[Db], reader: usize) {
    for db in dbs {
        db.flush_wal().await.unwrap();
    }
    dbs[reader].refresh().await.unwrap();
}

/// Runs a checkpoint on whoever holds the applier role, claiming it with
/// writer 0 if nobody does.
async fn checkpoint(dbs: &[Db]) {
    let applier = match dbs.iter().position(Db::is_applier) {
        Some(i) => i,
        None => {
            assert!(dbs[0].claim_applier().await.unwrap());
            0
        }
    };
    match dbs[applier].checkpoint().await {
        Ok(()) => assert_accounting(&dbs[applier]).await,
        // Lost the role to a concurrent claim: fine.
        Err(Error::NotApplier) => {}
        Err(e) => panic!("checkpoint: {e}"),
    }
}

async fn run(ops: Vec<Op>) {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = ManualClock::new();
    let mut dbs = Vec::new();
    for _ in 0..WRITERS {
        dbs.push(open(&os, &clock).await);
    }
    let mut model = Model::new();
    for op in ops {
        match op {
            Op::Put(w, k, v) => {
                clock.tick();
                dbs[w].put(key(k), v.clone()).await.unwrap();
                model.insert(key(k), Bytes::from(v));
            }
            Op::PutQueued(w, k, v) => {
                clock.tick();
                let mut batch = WriteBatch::new();
                batch.put(key(k), &v);
                let queued = WriteOptions {
                    durability: Durability::Queued,
                };
                dbs[w].write_with(batch, queued).await.unwrap();
                model.insert(key(k), Bytes::from(v));
            }
            Op::Delete(w, k) => {
                clock.tick();
                dbs[w].delete(key(k)).await.unwrap();
                model.remove(&key(k));
            }
            Op::Get(w, k) => {
                sync(&dbs, w).await;
                let got = dbs[w].get(&key(k)).await.unwrap();
                assert_eq!(got, model.get(&key(k)).cloned(), "get {k} via writer {w}");
            }
            Op::Scan(w, a, b) => {
                sync(&dbs, w).await;
                let (lo, hi) = (a.min(b), a.max(b));
                let ranges = [
                    KeyRange::new(Bound::Included(key(lo)), Bound::Excluded(key(hi))),
                    KeyRange::new(Bound::Excluded(key(lo)), Bound::Included(key(hi))),
                    KeyRange::new(Bound::Unbounded, Bound::Included(key(hi))),
                    KeyRange::new(Bound::Included(key(lo)), Bound::Unbounded),
                ];
                for r in ranges {
                    let got = dbs[w].scan(r.clone()).collect().await.unwrap();
                    assert_eq!(got, model_range(&model, &r), "scan {r:?} via writer {w}");
                }
            }
            Op::Checkpoint => checkpoint(&dbs).await,
            Op::Claim(w) => {
                dbs[w].claim_applier().await.unwrap();
            }
            Op::Reopen(w) => {
                let old = std::mem::replace(&mut dbs[w], open(&os, &clock).await);
                old.close().await.unwrap();
            }
            Op::ReaderScan => {
                sync(&dbs, 0).await;
                let reader = DbReader::builder(ROOT, os.clone())
                    .with_settings(reader_config())
                    .build()
                    .await
                    .unwrap();
                let got = reader.scan(KeyRange::all()).collect().await.unwrap();
                assert_eq!(got, model_range(&model, &KeyRange::all()), "reader scan");
            }
        }
    }
    sync(&dbs, 1).await;
    let all = dbs[1].scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
    checkpoint(&dbs).await;
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
    fn matches_model(ops in proptest::collection::vec(op(), 1..250)) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(run(ops));
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
    let height = db.stats().tree.unwrap().height;
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
    assert!(b.stats().unapplied_bytes == 0);
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
    let s = db.stats();
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
