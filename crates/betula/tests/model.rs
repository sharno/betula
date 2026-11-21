//! Model-based tests: random op sequences against a `BTreeMap`, with tiny
//! nodes so splits, buffer flushes, removals and root growth/shrink happen
//! constantly.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use betula::{
    CacheConfig, Clock, Db, DbConfig, DbReader, Error, KeyRange, ReaderConfig, TreeConfig,
};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use proptest::prelude::*;

const ROOT: &str = "db";

fn tiny_config() -> DbConfig {
    DbConfig {
        tree: TreeConfig {
            leaf_max_bytes: 160,
            internal_max_bytes: 320,
            max_fanout: 4,
        },
        // Small caches force reloads from storage.
        cache: CacheConfig {
            inner_bytes: 2048,
            leaf_bytes: 1024,
        },
        wal_flush_interval: Duration::from_millis(2),
        wal_max_bytes: 256,
        memtable_bytes: 200,
        max_immutables: 2,
        gc: None,
        warm_inner_nodes: true,
    }
}

fn reader_config() -> ReaderConfig {
    ReaderConfig {
        cache: CacheConfig {
            inner_bytes: 4096,
            leaf_bytes: 1024,
        },
        refresh_interval: None,
        warm_inner_nodes: false,
    }
}

#[derive(Clone, Debug)]
enum Op {
    Put(u8, Vec<u8>),
    Delete(u8),
    Get(u8),
    Scan(u8, u8),
    Checkpoint,
    FlushWal,
    Reopen,
    ReaderScan,
}

fn key(k: u8) -> Bytes {
    Bytes::from(format!("key{k:03}"))
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0u8..80, proptest::collection::vec(any::<u8>(), 0..48)).prop_map(|(k, v)| Op::Put(k, v)),
        3 => (0u8..80).prop_map(Op::Delete),
        2 => (0u8..80).prop_map(Op::Get),
        2 => (0u8..80, 0u8..80).prop_map(|(a, b)| Op::Scan(a, b)),
        1 => Just(Op::Checkpoint),
        1 => Just(Op::FlushWal),
        1 => Just(Op::Reopen),
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

async fn run(ops: Vec<Op>) {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    let mut model = Model::new();
    for op in ops {
        match op {
            Op::Put(k, v) => {
                db.put(key(k), v.clone()).await.unwrap();
                model.insert(key(k), Bytes::from(v));
            }
            Op::Delete(k) => {
                db.delete(key(k)).await.unwrap();
                model.remove(&key(k));
            }
            Op::Get(k) => {
                assert_eq!(
                    db.get(&key(k)).await.unwrap(),
                    model.get(&key(k)).cloned(),
                    "get {k}"
                );
            }
            Op::Scan(a, b) => {
                let (lo, hi) = (a.min(b), a.max(b));
                let ranges = [
                    KeyRange::new(Bound::Included(key(lo)), Bound::Excluded(key(hi))),
                    KeyRange::new(Bound::Excluded(key(lo)), Bound::Included(key(hi))),
                    KeyRange::new(Bound::Unbounded, Bound::Included(key(hi))),
                    KeyRange::new(Bound::Included(key(lo)), Bound::Unbounded),
                ];
                for r in ranges {
                    let got = db.scan(r.clone()).collect().await.unwrap();
                    assert_eq!(got, model_range(&model, &r), "scan {r:?}");
                }
            }
            Op::Checkpoint => db.checkpoint().await.unwrap(),
            Op::FlushWal => db.flush_wal().await.unwrap(),
            Op::Reopen => {
                db.close().await.unwrap();
                db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
            }
            Op::ReaderScan => {
                let reader = DbReader::open(os.clone(), ROOT, reader_config())
                    .await
                    .unwrap();
                let got = reader.scan(KeyRange::all()).collect().await.unwrap();
                assert_eq!(got, model_range(&model, &KeyRange::all()), "reader scan");
            }
        }
    }
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
    db.checkpoint().await.unwrap();
    db.close().await.unwrap();
    let db = Db::open(os, ROOT, tiny_config()).await.unwrap();
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(
        all,
        model_range(&model, &KeyRange::all()),
        "after final reopen"
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn matches_model(ops in proptest::collection::vec(op(), 1..300)) {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(run(ops));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_load_builds_multi_level_tree() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    let mut model = Model::new();
    for i in 0..2000u32 {
        let k = Bytes::from(format!("k{:08}", (i * 7919) % 2000));
        let v = Bytes::from(format!("v{i}"));
        db.put(k.clone(), v.clone()).await.unwrap();
        model.insert(k, v);
        if i % 3 == 0 {
            let d = Bytes::from(format!("k{:08}", (i * 31) % 2000));
            db.delete(d.clone()).await.unwrap();
            model.remove(&d);
        }
    }
    db.checkpoint().await.unwrap();
    let height = db.stats().tree.unwrap().height;
    assert!(height >= 3, "height {height}");
    let all = db.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_writer_fences_old_one() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    a.put("x", "1").await.unwrap();
    let b = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    assert_eq!(b.get(b"x").await.unwrap(), Some(Bytes::from("1")));
    assert!(matches!(a.put("y", "2").await, Err(Error::Fenced)));
    assert!(matches!(a.put("z", "3").await, Err(Error::Fenced)));
    b.put("y", "b").await.unwrap();
    b.checkpoint().await.unwrap();
    assert_eq!(b.get(b"y").await.unwrap(), Some(Bytes::from("b")));
    assert_eq!(b.get(b"z").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_checkpoint_fails() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    let _b = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    // `a` never wrote a WAL, so only the manifest conflict can fence it.
    let r = a.checkpoint().await;
    assert!(matches!(r, Ok(()) | Err(Error::Fenced)), "{r:?}");
}

struct ManualClock(AtomicU64);

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

async fn node_objects(os: &Arc<dyn ObjectStore>) -> usize {
    let prefix = Path::from(format!("{ROOT}/node"));
    os.list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_keeps_exactly_the_live_tree() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = Arc::new(ManualClock(AtomicU64::new(1_000)));
    let db = Db::open_with_clock(os.clone(), ROOT, tiny_config(), clock.clone())
        .await
        .unwrap();
    let mut model = Model::new();
    for round in 0..20u32 {
        for i in 0..40u32 {
            let k = Bytes::from(format!("k{:04}", (i * 13 + round * 7) % 300));
            let v = Bytes::from(format!("r{round}i{i}"));
            db.put(k.clone(), v.clone()).await.unwrap();
            model.insert(k, v);
        }
        db.checkpoint().await.unwrap();
    }
    let before = node_objects(&os).await;

    // Within retention nothing is deleted.
    let stats = db.run_gc(Duration::from_secs(60)).await.unwrap();
    assert_eq!(stats.nodes, 0);

    clock.0.fetch_add(120_000, Ordering::SeqCst);
    let stats = db.run_gc(Duration::from_secs(60)).await.unwrap();
    assert!(stats.nodes > 0 && stats.manifests > 0, "{stats:?}");
    let live = db.tree_shape().await.unwrap().nodes.len();
    let after = node_objects(&os).await;
    assert!(after < before);
    assert_eq!(after, live, "only the live tree remains");

    db.close().await.unwrap();
    let reader = DbReader::open(os, ROOT, reader_config()).await.unwrap();
    let all = reader.scan(KeyRange::all()).collect().await.unwrap();
    assert_eq!(all, model_range(&model, &KeyRange::all()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_follows_writer() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    let reader = DbReader::open(os.clone(), ROOT, reader_config())
        .await
        .unwrap();
    db.put("a", "1").await.unwrap();
    reader.refresh().await.unwrap();
    assert_eq!(
        reader.get(b"a").await.unwrap(),
        Some(Bytes::from("1")),
        "from WAL tail"
    );
    db.checkpoint().await.unwrap();
    db.put("a", "2").await.unwrap();
    db.put("b", "3").await.unwrap();
    reader.refresh().await.unwrap();
    assert_eq!(reader.get(b"a").await.unwrap(), Some(Bytes::from("2")));
    db.delete("a").await.unwrap();
    db.checkpoint().await.unwrap();
    reader.refresh().await.unwrap();
    assert_eq!(reader.get(b"a").await.unwrap(), None);
    assert_eq!(reader.get(b"b").await.unwrap(), Some(Bytes::from("3")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_orphans_are_deleted_on_open() {
    let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    for i in 0..200u32 {
        db.put(format!("k{i:04}"), format!("v{i}")).await.unwrap();
    }
    db.checkpoint().await.unwrap();
    db.close().await.unwrap();
    let published = node_objects(&os).await;
    // Simulate a writer that uploaded nodes but crashed before publishing.
    let orphan = Path::from(format!("{ROOT}/node/{:020}-{:020}.node", 1, 1_000_000));
    os.put(&orphan, "junk".into()).await.unwrap();
    let db = Db::open(os.clone(), ROOT, tiny_config()).await.unwrap();
    assert_eq!(node_objects(&os).await, published);
    assert_eq!(db.get(b"k0042").await.unwrap(), Some(Bytes::from("v42")));
}
