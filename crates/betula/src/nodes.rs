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
use crate::types::{NodeId, TreeRoot};

const UPLOAD_CONCURRENCY: usize = 64;
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

impl Weighter<NodeId, (Node, u32)> for EncodedSize {
    fn weight(&self, _: &NodeId, (_, size): &(Node, u32)) -> u64 {
        u64::from(*size).max(1)
    }
}

type NodeCache = Cache<NodeId, (Node, u32), EncodedSize>;

/// A fetch shared by every concurrent reader of the same missing node.
type Loading = Shared<BoxFuture<'static, Result<(Node, usize)>>>;

pub struct Nodes {
    store: Store,
    inner: NodeCache,
    leaves: NodeCache,
    loading: Mutex<HashMap<NodeId, Loading>>,
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

    fn insert(&self, id: NodeId, node: Node, size: usize) {
        let size = u32::try_from(size).unwrap_or(u32::MAX);
        self.cache_for(&node).insert(id, (node, size));
    }

    pub async fn load(&self, id: NodeId) -> Result<Node> {
        if let Some((n, _)) = self.inner.get(&id).or_else(|| self.leaves.get(&id)) {
            return Ok(n);
        }
        let loading = self
            .loading
            .lock()
            .entry(id)
            .or_insert_with(|| fetch(self.store.clone(), id).boxed().shared())
            .clone();
        let result = loading.await;
        // Cache before dropping the in-flight entry so no reader slips
        // between the two and fetches again.
        if let Ok((node, size)) = &result {
            self.insert(id, node.clone(), *size);
        }
        self.loading.lock().remove(&id);
        result.map(|(node, _)| node)
    }

    /// Drops nodes the current tree no longer uses.
    pub fn evict(&self, ids: &[NodeId]) {
        ids.iter().for_each(|id| {
            self.inner.remove(id);
            self.leaves.remove(id);
        });
    }

    /// Uploads new nodes and caches them, so the writer never re-reads its own
    /// writes.
    pub async fn upload(&self, nodes: &[(NodeId, Node)]) -> Result<()> {
        // Owned items: a borrowing closure trips higher-ranked lifetime
        // inference once this future is spawned.
        stream::iter(nodes.to_vec())
            .map(|(id, node)| async move {
                let data: Bytes = codec::encode(codec::NODE, &node);
                let size = data.len();
                self.store.put(&self.store.node_path(id), data).await?;
                self.insert(id, node, size);
                Ok::<_, Error>(())
            })
            .buffer_unordered(UPLOAD_CONCURRENCY)
            .try_collect()
            .await
    }
}

async fn fetch(store: Store, id: NodeId) -> Result<(Node, usize)> {
    let path = store.node_path(id);
    let data = store.get(&path).await?.ok_or(Error::SnapshotExpired(id))?;
    let node = codec::decode(codec::NODE, path.as_ref(), &data)?;
    Ok((node, data.len()))
}

impl Nodes {
    /// Loads every internal node of the tree, one level at a time.
    pub async fn warm_inner(&self, root: Option<TreeRoot>) -> Result<usize> {
        let Some(root) = root else { return Ok(0) };
        let mut level = vec![root.id];
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
