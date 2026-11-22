//! Node loading and caching. Nodes are immutable, so cache entries never need
//! invalidation. Internal nodes get their own cache, sized to hold every
//! internal node of the tree, so a point read costs at most one GET (the leaf).

use std::collections::HashMap;

use bytes::Bytes;
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use parking_lot::Mutex;
use quick_cache::sync::Cache;
use quick_cache::{OptionsBuilder, Weighter};

use crate::codec;
use crate::error::{Error, Result};
use crate::node::Node;
use crate::store::Store;
use crate::types::{NodePtr, SegmentId, TreeRoot};

const LOAD_CONCURRENCY: usize = 16;
/// Conservative size estimates for cache sharding. quick_cache splits its
/// capacity evenly across shards, so a shard must fit many of the largest
/// items or it evicts while the cache as a whole has room.
const INNER_NODE_ESTIMATE: u64 = 1 << 20;
const LEAF_ESTIMATE: u64 = 128 << 10;
const ITEMS_PER_SHARD: u64 = 192;

#[derive(Clone, Copy, Debug)]
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

pub struct Nodes {
    store: Store,
    inner: NodeCache,
    leaves: NodeCache,
    loading: Mutex<HashMap<NodePtr, Loading>>,
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
        Self {
            store,
            inner: new_cache(cfg.inner_bytes, INNER_NODE_ESTIMATE),
            leaves: new_cache(cfg.leaf_bytes, LEAF_ESTIMATE),
            loading: Mutex::new(HashMap::new()),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    fn cache_for(&self, node: &Node) -> &NodeCache {
        match node {
            Node::Leaf(_) => &self.leaves,
            Node::Internal(_) => &self.inner,
        }
    }

    fn insert(&self, ptr: NodePtr, node: Node) {
        self.cache_for(&node).insert(ptr, node);
    }

    pub async fn load(&self, ptr: NodePtr) -> Result<Node> {
        if let Some(n) = self.inner.get(&ptr).or_else(|| self.leaves.get(&ptr)) {
            return Ok(n);
        }
        let loading = self
            .loading
            .lock()
            .entry(ptr)
            .or_insert_with(|| fetch(self.store.clone(), ptr).boxed().shared())
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

async fn fetch(store: Store, ptr: NodePtr) -> Result<Node> {
    let path = store.segment_path(ptr.seg);
    let data = store
        .get_range(&path, ptr.range())
        .await?
        .ok_or(Error::SnapshotExpired(ptr.seg))?;
    codec::decode(codec::NODE, &format!("{path}@{}", ptr.offset), &data)
}

impl Nodes {
    /// Loads every internal node of the tree, one level at a time.
    pub async fn warm_inner(&self, root: Option<TreeRoot>) -> Result<usize> {
        let Some(root) = root else { return Ok(0) };
        let mut level = vec![root.ptr];
        let mut loaded = 0;
        for _ in 0..root.height {
            let nodes: Vec<Node> = stream::iter(level)
                .map(|id| self.load(id))
                .buffer_unordered(LOAD_CONCURRENCY)
                .try_collect()
                .await?;
            loaded += nodes.len();
            level = nodes
                .iter()
                .flat_map(|n| match n {
                    Node::Internal(i) => i.children.clone(),
                    Node::Leaf(_) => Vec::new(),
                })
                .collect();
        }
        Ok(loaded)
    }
}
