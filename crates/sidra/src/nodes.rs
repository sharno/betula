//! Node loading and caching. Nodes are immutable, so cache entries never need
//! invalidation. Internal nodes get their own cache, sized to hold every
//! internal node of the tree, so a point read costs at most one GET (the leaf).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use quick_cache::sync::Cache;
use quick_cache::{OptionsBuilder, Weighter};

use crate::codec;
use crate::error::{Error, Result};
use crate::node::{Internal, Node};
use crate::store::Store;
use crate::types::{NodePtr, SegmentId, TreeRoot};

const LOAD_CONCURRENCY: usize = 64;
const BATCH_BYTES: u64 = 1 << 20;
/// Conservative size estimates for cache sharding. quick_cache splits its
/// capacity evenly across shards, so a shard must fit many of the largest
/// items or it evicts while the cache as a whole has room.
const INNER_NODE_ESTIMATE: u64 = 1 << 20;
const LEAF_ESTIMATE: u64 = 128 << 10;
const ITEMS_PER_SHARD: u64 = 192;

/// How to load a tree version's internal nodes and buffered runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Warm {
    /// Load on first use.
    Off,
    /// Load internal nodes before returning and runs in the background:
    /// fast open (about 4x faster); reads that need a run before it arrives
    /// wait for its batch. Suits processes that start often.
    Background,
    /// Load everything before returning: slower open, full-speed reads from
    /// the first request. Suits long-running services.
    #[default]
    Blocking,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub inner_bytes: u64,
    pub leaf_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            inner_bytes: 1 << 30,
            leaf_bytes: 256 << 20,
        }
    }
}

#[derive(Clone)]
struct EncodedSize;

impl Weighter<NodePtr, Node> for EncodedSize {
    fn weight(&self, ptr: &NodePtr, _: &Node) -> u64 {
        u64::from(ptr.len).max(1)
    }
}

type NodeCache = Cache<NodePtr, Node, EncodedSize>;

/// A fetch shared by every concurrent reader of the same missing node.
type Loading = Shared<BoxFuture<'static, Result<Node>>>;

/// Node cache occupancy and lookups since open. A miss is a node read that
/// went to the object store (or joined an in-flight fetch).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub inner_bytes: u64,
    pub leaf_bytes: u64,
    pub hits: u64,
    pub misses: u64,
}

/// Where segments are read from.
#[derive(Clone)]
enum Source {
    Home,
    /// A replica of the home store (bucket replication), which may lag: a
    /// segment not there yet is read from the origin.
    Replica {
        origin: Store,
    },
}

pub struct Nodes {
    store: Store,
    source: Source,
    inner: NodeCache,
    leaves: NodeCache,
    loading: Mutex<HashMap<NodePtr, Loading>>,
    /// Nodes of a planned checkpoint not uploaded yet, which the next plan
    /// builds on. Pinned here because the cache may evict them.
    staged: parking_lot::RwLock<HashMap<NodePtr, Node>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

fn new_cache(bytes: u64, item_estimate: u64) -> NodeCache {
    let items = (bytes / item_estimate).max(1);
    let shards = (items / ITEMS_PER_SHARD).clamp(1, 32) as usize;
    let options = OptionsBuilder::new()
        .estimated_items_capacity(items as usize)
        .weight_capacity(bytes.max(1))
        .shards(shards)
        .build()
        .expect("cache options are complete");
    Cache::with_options(options, EncodedSize, Default::default(), Default::default())
}

impl Nodes {
    pub fn new(store: Store, cfg: CacheConfig) -> Self {
        Self::with_source(store, Source::Home, cfg)
    }

    /// Reads from `replica`, falling back to `origin` for segments that
    /// haven't replicated yet.
    pub fn replica(replica: Store, origin: Store, cfg: CacheConfig) -> Self {
        Self::with_source(replica, Source::Replica { origin }, cfg)
    }

    fn with_source(store: Store, source: Source, cfg: CacheConfig) -> Self {
        Self {
            store,
            source,
            inner: new_cache(cfg.inner_bytes, INNER_NODE_ESTIMATE),
            leaves: new_cache(cfg.leaf_bytes, LEAF_ESTIMATE),
            loading: Mutex::new(HashMap::new()),
            staged: parking_lot::RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            inner_bytes: self.inner.weight(),
            leaf_bytes: self.leaves.weight(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    fn cache_for(&self, node: &Node) -> &NodeCache {
        match node {
            Node::Leaf(_) => &self.leaves,
            // Runs are part of the Bε buffers: needed by every read below them.
            Node::Internal(_) | Node::Run(_) => &self.inner,
        }
    }

    fn insert(&self, ptr: NodePtr, node: Node) {
        self.cache_for(&node).insert(ptr, node);
    }

    pub async fn load(&self, ptr: NodePtr) -> Result<Node> {
        let cached = self
            .inner
            .get(&ptr)
            .or_else(|| self.leaves.get(&ptr))
            .or_else(|| self.staged.read().get(&ptr).cloned());
        if let Some(n) = cached {
            self.hits.fetch_add(1, Ordering::Relaxed);
            // See `Snapshot::get`: reads served from memory must yield.
            tokio::task::consume_budget().await;
            return Ok(n);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let loading = self
            .loading
            .lock()
            .entry(ptr)
            .or_insert_with(|| {
                fetch(self.store.clone(), self.source.clone(), ptr)
                    .boxed()
                    .shared()
            })
            .clone();
        let result = loading.await;
        // Cache before dropping the in-flight entry so no reader slips
        // between the two and fetches again.
        if let Ok(node) = &result {
            self.insert(ptr, node.clone());
        }
        self.loading.lock().remove(&ptr);
        result
    }

    /// Drops nodes the current tree no longer uses.
    pub fn evict(&self, ptrs: &[NodePtr]) {
        ptrs.iter().for_each(|ptr| {
            self.inner.remove(ptr);
            self.leaves.remove(ptr);
        });
    }

    /// Makes a planned segment's nodes loadable before it's uploaded.
    pub fn stage(&self, segments: &[Segment]) {
        self.staged.write().extend(
            segments
                .iter()
                .flat_map(|s| s.nodes.iter().map(|(p, n)| (*p, n.clone()))),
        );
    }

    /// Drops staged nodes of a plan that won't be published.
    pub fn unstage(&self, segments: &[Segment]) {
        let mut staged = self.staged.write();
        segments
            .iter()
            .flat_map(|s| s.nodes.iter())
            .for_each(|(p, _)| {
                staged.remove(p);
            });
    }

    /// Uploads a checkpoint's segment and caches its nodes, so the writer
    /// never re-reads its own writes.
    pub async fn upload(&self, segment: &Segment) -> Result<()> {
        self.store
            .put(&self.store.segment_path(segment.id), segment.data.clone())
            .await?;
        segment
            .nodes
            .iter()
            .for_each(|(ptr, node)| self.insert(*ptr, node.clone()));
        self.unstage(std::slice::from_ref(segment));
        Ok(())
    }
}

/// Every node a checkpoint wrote, encoded back to back.
#[derive(Clone, Debug)]
pub struct Segment {
    pub id: SegmentId,
    pub data: Bytes,
    pub nodes: Vec<(NodePtr, Node)>,
}

/// Nodes of one segment, fetched with one coalesced ranged read.
async fn fetch_batch(
    store: Store,
    source: Source,
    seg: SegmentId,
    ptrs: Vec<NodePtr>,
) -> Result<Vec<Node>> {
    let path = store.segment_path(seg);
    let ranges: Vec<_> = ptrs.iter().map(NodePtr::range).collect();
    let local = store.get_ranges(&path, &ranges).await?;
    let data = match (local, source) {
        (Some(data), _) => Some(data),
        (None, Source::Home) => None,
        (None, Source::Replica { origin }) => {
            origin
                .get_ranges(&origin.segment_path(seg), &ranges)
                .await?
        }
    }
    .ok_or(Error::SnapshotExpired(seg))?;
    ptrs.iter()
        .zip(data)
        .map(|(ptr, bytes)| codec::decode(codec::NODE, &format!("{path}@{}", ptr.offset), &bytes))
        .collect()
}

async fn fetch(store: Store, source: Source, ptr: NodePtr) -> Result<Node> {
    let path = store.segment_path(ptr.seg);
    let local = store.get_range(&path, ptr.range()).await?;
    let data = match (local, source) {
        (Some(data), _) => Some(data),
        (None, Source::Home) => None,
        (None, Source::Replica { origin }) => {
            origin
                .get_range(&origin.segment_path(ptr.seg), ptr.range())
                .await?
        }
    }
    .ok_or(Error::SnapshotExpired(ptr.seg))?;
    codec::decode(codec::NODE, &format!("{path}@{}", ptr.offset), &data)
}

impl Nodes {
    /// Loads every internal node of the tree, one level at a time.
    /// Loads many nodes, fetching uncached ones per segment with one
    /// coalesced ranged read (nearby ranges merge into one request). Each
    /// node in a batch is registered as in flight, so a concurrent `load` of
    /// it waits for the batch instead of fetching it again.
    pub async fn load_many(&self, ptrs: &[NodePtr]) -> Result<Vec<Node>> {
        {
            let mut loading = self.loading.lock();
            let missing: BTreeMap<SegmentId, Vec<NodePtr>> = ptrs
                .iter()
                .filter(|&&p| {
                    self.inner.get(&p).or_else(|| self.leaves.get(&p)).is_none()
                        && !loading.contains_key(&p)
                })
                .fold(BTreeMap::new(), |mut m, p| {
                    m.entry(p.seg).or_default().push(*p);
                    m
                });
            // Split each segment's nodes into batches of about BATCH_BYTES, so
            // a read waiting on one node waits for a small batch.
            let batches = missing.into_iter().flat_map(|(seg, mut group)| {
                group.sort();
                group
                    .into_iter()
                    .fold(Vec::<(u64, Vec<NodePtr>)>::new(), |mut acc, p| {
                        match acc.last_mut() {
                            Some((bytes, b)) if *bytes < BATCH_BYTES => {
                                *bytes += u64::from(p.len);
                                b.push(p);
                            }
                            _ => acc.push((u64::from(p.len), vec![p])),
                        }
                        acc
                    })
                    .into_iter()
                    .map(move |(_, b)| (seg, b))
            });
            for (seg, group) in batches {
                let batch =
                    fetch_batch(self.store.clone(), self.source.clone(), seg, group.clone())
                        .boxed()
                        .shared();
                group.iter().enumerate().for_each(|(i, p)| {
                    let one = batch
                        .clone()
                        .map(move |r| r.map(|nodes| nodes[i].clone()))
                        .boxed()
                        .shared();
                    loading.entry(*p).or_insert(one);
                });
            }
        }
        stream::iter(ptrs.iter().copied())
            .map(|p| self.load(p))
            .buffered(LOAD_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Loads every internal node of the tree, one level at a time, and
    /// returns the runs they reference.
    pub async fn warm_inner(&self, root: Option<TreeRoot>) -> Result<Vec<NodePtr>> {
        let Some(root) = root else {
            return Ok(Vec::new());
        };
        let mut level = vec![root.ptr];
        let mut runs = Vec::new();
        for _ in 0..root.height {
            let nodes = self.load_many(&level).await?;
            let internals: Vec<&Internal> = nodes
                .iter()
                .filter_map(|n| match n {
                    Node::Internal(i) => Some(i.as_ref()),
                    Node::Leaf(_) | Node::Run(_) => None,
                })
                .collect();
            runs.extend(internals.iter().flat_map(|i| i.run_ptrs()));
            level = internals
                .iter()
                .flat_map(|i| i.children.iter().map(|c| c.ptr))
                .collect();
        }
        Ok(runs)
    }

    /// Loads the tree's internal nodes, and its runs either before
    /// returning or in the background.
    pub async fn warm(self: &Arc<Self>, root: Option<TreeRoot>, mode: Warm) -> Result<()> {
        if mode == Warm::Off {
            return Ok(());
        }
        let runs = self.warm_inner(root).await?;
        match mode {
            Warm::Off => {}
            Warm::Blocking => {
                self.load_many(&runs).await?;
            }
            Warm::Background => {
                let nodes = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = nodes.load_many(&runs).await {
                        tracing::debug!(%e, "background run warmup failed");
                    }
                });
            }
        }
        Ok(())
    }
}
