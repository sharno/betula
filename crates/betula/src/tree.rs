//! Bε-tree reads and copy-on-write batch application.
//!
//! A batch lands in the root's buffer. When a buffer exceeds its capacity, the
//! messages destined for the child with the most pending bytes are flushed into
//! that child, recursively. Every node touched gets a new id; untouched
//! subtrees are shared with the previous version.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream};

use crate::codec;
use crate::error::{Error, Result};
use crate::hlc::Ts;
use crate::node::{
    Child, Internal, Leaf, Node, apply_to_leaf, chunk_by_bytes, group_starts, merge_versions,
    msgs_bytes,
};
use crate::nodes::Nodes;
use crate::nodes::Segment;
use crate::types::{KeyRange, NodePtr, SegmentId, TreeRoot, Version, entry_bytes};

#[derive(Clone, Copy, Debug)]
pub struct TreeConfig {
    /// Leaves larger than this are split.
    pub leaf_max_bytes: usize,
    /// Internal node budget; the buffer gets whatever pivots don't use.
    pub internal_max_bytes: usize,
    /// Internal nodes with more children are split. This is the B^ε fanout.
    pub max_fanout: usize,
}

impl Default for TreeConfig {
    fn default() -> Self {
        Self {
            leaf_max_bytes: 512 << 10,
            internal_max_bytes: 4 << 20,
            max_fanout: 64,
        }
    }
}

impl TreeConfig {
    fn buffer_capacity(&self, node: &Internal) -> usize {
        // Keep a floor so a node full of long pivots still buffers something.
        self.internal_max_bytes
            .saturating_sub(node.routing_bytes())
            .max(self.internal_max_bytes / 4)
    }

    fn leaf_split_target(&self) -> usize {
        self.leaf_max_bytes * 3 / 4
    }
}

/// The newest version of `key` in the tree. Walks the whole root-to-leaf
/// path: a late write from a slow writer can sit above a newer version.
pub async fn get(nodes: &Nodes, root: Option<TreeRoot>, key: &[u8]) -> Result<Option<Version>> {
    let Some(root) = root else { return Ok(None) };
    let mut id = root.ptr;
    let mut best = None;
    loop {
        match nodes.load(id).await? {
            Node::Leaf(leaf) => return Ok(Version::newest(best, leaf.get(key).cloned())),
            Node::Internal(node) => {
                best = Version::newest(best, node.buffered(key).cloned());
                id = node.children[node.child_index(key)].ptr;
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TreeShape {
    pub internal: usize,
    pub leaves: usize,
    /// Tombstones still held in leaves.
    pub tombstones: usize,
    /// Every reachable node.
    pub nodes: Vec<NodePtr>,
}

/// Walks the whole tree. Diagnostic: loads every leaf.
pub async fn shape(nodes: &Nodes, root: Option<TreeRoot>) -> Result<TreeShape> {
    let mut shape = TreeShape::default();
    let mut level: Vec<NodePtr> = root.map(|r| r.ptr).into_iter().collect();
    while !level.is_empty() {
        let loaded: Vec<(NodePtr, Node)> = stream::iter(level)
            .map(|id| async move { Ok::<_, Error>((id, nodes.load(id).await?)) })
            .buffered(16)
            .try_collect()
            .await?;
        level = loaded
            .iter()
            .flat_map(|(_, n)| match n {
                Node::Internal(i) => i.children.iter().map(|c| c.ptr).collect(),
                Node::Leaf(_) => Vec::new(),
            })
            .collect();
        let internal = loaded
            .iter()
            .filter(|(_, n)| matches!(n, Node::Internal(_)))
            .count();
        shape.tombstones += loaded
            .iter()
            .filter_map(|(_, n)| match n {
                Node::Leaf(l) => Some(
                    l.entries
                        .iter()
                        .filter(|(_, v)| v.value().is_none())
                        .count(),
                ),
                Node::Internal(_) => None,
            })
            .sum::<usize>();
        shape.internal += internal;
        shape.leaves += loaded.len() - internal;
        shape.nodes.extend(loaded.into_iter().map(|(id, _)| id));
    }
    Ok(shape)
}

// ---------------------------------------------------------------------------
// Range iteration

struct Frame {
    node: Arc<Internal>,
    idx: usize,
    last: usize,
}

enum IterState {
    Start(Option<TreeRoot>),
    Walking(Vec<Frame>),
    Done,
}

/// Visits leaves in key order. Each leaf's view is its entries overlaid with
/// ancestor buffer messages in the leaf's key range, upper levels winning.
pub struct TreeIter {
    nodes: Arc<Nodes>,
    range: KeyRange,
    state: IterState,
}

impl TreeIter {
    pub fn new(nodes: Arc<Nodes>, root: Option<TreeRoot>, range: KeyRange) -> Self {
        Self {
            nodes,
            range,
            state: IterState::Start(root),
        }
    }

    /// The next non-empty run of live entries, in key order.
    /// The next non-empty run of versions (tombstones included, so they can
    /// shadow log tails), in key order.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<(Bytes, Version)>>> {
        loop {
            let (stack, leaf) = match std::mem::replace(&mut self.state, IterState::Done) {
                IterState::Done | IterState::Start(None) => return Ok(None),
                IterState::Start(Some(root)) => {
                    let mut stack = Vec::new();
                    let leaf = self.descend(root.ptr, &mut stack).await?;
                    (stack, leaf)
                }
                IterState::Walking(mut stack) => match self.advance(&mut stack).await? {
                    Some(leaf) => (stack, leaf),
                    None => return Ok(None),
                },
            };
            let chunk = self.materialize(&stack, &leaf);
            self.state = IterState::Walking(stack);
            if !chunk.is_empty() {
                return Ok(Some(chunk));
            }
        }
    }

    async fn descend(&self, mut id: NodePtr, stack: &mut Vec<Frame>) -> Result<Arc<Leaf>> {
        loop {
            match self.nodes.load(id).await? {
                Node::Leaf(leaf) => return Ok(leaf),
                Node::Internal(node) => {
                    let idx = node.first_child(&self.range.start);
                    let last = node.last_child(&self.range.end).max(idx);
                    id = node.children[idx].ptr;
                    stack.push(Frame { node, idx, last });
                }
            }
        }
    }

    async fn advance(&self, stack: &mut Vec<Frame>) -> Result<Option<Arc<Leaf>>> {
        while let Some(top) = stack.last_mut() {
            if top.idx < top.last {
                top.idx += 1;
                let id = top.node.children[top.idx].ptr;
                return self.descend(id, stack).await.map(Some);
            }
            stack.pop();
        }
        Ok(None)
    }

    fn materialize(&self, stack: &[Frame], leaf: &Leaf) -> Vec<(Bytes, Version)> {
        // The leaf's key range is the intersection of every ancestor's child
        // bounds; a frame's own bound is `None` where it inherits its parent's.
        let (lo, hi) = stack.iter().fold((None, None), |(lo, hi), f| {
            let (flo, fhi) = f.node.child_bounds(f.idx);
            (flo.or(lo), fhi.or(hi))
        });
        let in_leaf = |k: &Bytes| {
            lo.is_none_or(|lo: &Bytes| k >= lo)
                && hi.is_none_or(|hi: &Bytes| k < hi)
                && self.range.contains(k)
        };
        let base: BTreeMap<Bytes, Version> = leaf
            .entries
            .iter()
            .filter(|(k, _)| self.range.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        stack
            .iter()
            .flat_map(|f| f.node.buffer.iter().filter(|(k, _)| in_leaf(k)))
            .fold(base, |mut view, (k, v)| {
                if view.get(k).is_none_or(|old| v.ts > old.ts) {
                    view.insert(k.clone(), v.clone());
                }
                view
            })
            .into_iter()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Copy-on-write application

/// Result of rewriting a subtree.
enum Rewrite {
    /// The subtree became empty.
    Removed,
    /// The subtree is now `first` followed by `rest`, each tagged with its
    /// lowest key (the pivot to insert before it).
    Nodes {
        first: Child,
        rest: Vec<(Bytes, Child)>,
    },
}

/// A new tree version, not yet uploaded or published.
pub struct Plan {
    pub root: Option<TreeRoot>,
    /// New nodes, or `None` if nothing changed.
    pub segment: Option<Segment>,
    /// Nodes of the previous version that the new version no longer uses.
    pub freed: Vec<NodePtr>,
}

/// Applies sorted, key-unique `msgs` to the tree at `root`, writing new nodes
/// into segment `seg`. Live nodes in `victims` segments are rewritten too,
/// so those segments can be deleted once nothing else references them.
pub async fn apply(
    nodes: &Nodes,
    cfg: &TreeConfig,
    seg: SegmentId,
    root: Option<TreeRoot>,
    msgs: Vec<(Bytes, Version)>,
    victims: &HashSet<SegmentId>,
    drop_tombstones_below: Ts,
) -> Result<Plan> {
    let mut w = Writer {
        nodes,
        cfg,
        victims,
        drop_below: drop_tombstones_below,
        dirty: HashSet::new(),
        seg,
        buf: BytesMut::new(),
        written: Vec::new(),
        freed: Vec::new(),
    };
    if let Some(r) = root.filter(|_| !victims.is_empty()) {
        w.mark_dirty(r.ptr, r.height).await?;
    }
    let root = w.apply_root(root, msgs).await?;
    let segment = (!w.written.is_empty()).then(|| Segment {
        id: seg,
        data: w.buf.freeze(),
        nodes: w.written,
    });
    Ok(Plan {
        root,
        segment,
        freed: w.freed,
    })
}

struct Writer<'a> {
    nodes: &'a Nodes,
    cfg: &'a TreeConfig,
    victims: &'a HashSet<SegmentId>,
    /// Tombstones older than this are dropped when their leaf is rewritten.
    drop_below: Ts,
    /// Internal nodes with a node in `victims` at or below them.
    dirty: HashSet<NodePtr>,
    seg: SegmentId,
    buf: BytesMut,
    written: Vec<(NodePtr, Node)>,
    freed: Vec<NodePtr>,
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

impl Writer<'_> {
    fn needs_purge(&self, oldest_tombstone: Option<Ts>) -> bool {
        oldest_tombstone.is_some_and(|t| t < self.drop_below)
    }

    /// Whether the subtree at `ptr` holds a node that must move.
    fn needs_move(&self, ptr: NodePtr) -> bool {
        self.victims.contains(&ptr.seg) || self.dirty.contains(&ptr)
    }

    /// Finds internal nodes on paths to victim nodes. Walks only internal
    /// nodes (cached); a leaf is judged by the pointer its parent holds.
    fn mark_dirty(&mut self, ptr: NodePtr, height: u32) -> BoxFut<'_, bool> {
        Box::pin(async move {
            if height == 0 {
                return Ok(self.victims.contains(&ptr.seg));
            }
            let Node::Internal(node) = self.nodes.load(ptr).await? else {
                return Ok(self.victims.contains(&ptr.seg));
            };
            let mut dirty = self.victims.contains(&ptr.seg);
            for child in &node.children {
                dirty |= self.mark_dirty(child.ptr, height - 1).await?;
            }
            if dirty {
                self.dirty.insert(ptr);
            }
            Ok(dirty)
        })
    }

    async fn apply_root(
        &mut self,
        root: Option<TreeRoot>,
        msgs: Vec<(Bytes, Version)>,
    ) -> Result<Option<TreeRoot>> {
        let (mut level, mut height) = match root {
            None => {
                let entries = apply_to_leaf(Vec::new(), msgs, self.drop_below);
                (self.write_leaves(entries), 0)
            }
            Some(r)
                if msgs.is_empty()
                    && !self.needs_move(r.ptr)
                    && !self.needs_purge(r.oldest_tombstone) =>
            {
                return Ok(Some(r));
            }
            Some(r) => (self.rewrite(r.ptr, msgs).await?, r.height),
        };
        // Grow upward while the top level still has several nodes.
        let mut root = loop {
            match level {
                Rewrite::Removed => return Ok(None),
                Rewrite::Nodes { first, rest } if rest.is_empty() => {
                    break TreeRoot {
                        ptr: first.ptr,
                        oldest_tombstone: first.oldest_tombstone,
                        height,
                    };
                }
                Rewrite::Nodes { first, rest } => {
                    let (pivots, more): (Vec<_>, Vec<_>) = rest.into_iter().unzip();
                    let children = std::iter::once(first).chain(more).collect();
                    level = self.write_internals(Internal {
                        pivots,
                        children,
                        buffer: Vec::new(),
                    });
                    height += 1;
                }
            }
        };
        // Shrink: a root with one child and nothing buffered is just a hop.
        // The root is always the node written last, so unwrite it.
        while let Some((ptr, Node::Internal(node))) = self.written.last() {
            if *ptr != root.ptr || node.children.len() != 1 || !node.buffer.is_empty() {
                break;
            }
            let only = node.children[0];
            root = TreeRoot {
                ptr: only.ptr,
                oldest_tombstone: only.oldest_tombstone,
                height: root.height - 1,
            };
            self.buf.truncate(ptr.offset as usize);
            self.written.pop();
        }
        Ok(Some(root))
    }

    /// Rewrites the node at `ptr` with `msgs` applied, moving any victim
    /// nodes below it and dropping tombstones that became droppable.
    fn rewrite(&mut self, ptr: NodePtr, msgs: Vec<(Bytes, Version)>) -> BoxFut<'_, Rewrite> {
        Box::pin(async move {
            let node = self.nodes.load(ptr).await?;
            self.freed.push(ptr);
            match node {
                Node::Leaf(leaf) => Ok(self.write_leaves(apply_to_leaf(
                    leaf.entries.clone(),
                    msgs,
                    self.drop_below,
                ))),
                Node::Internal(old) => {
                    let mut node = Internal {
                        pivots: old.pivots.clone(),
                        children: old.children.clone(),
                        buffer: merge_versions(old.buffer.clone(), msgs),
                    };
                    while node.buffer_bytes() > self.cfg.buffer_capacity(&node) {
                        let pending = node.pending_per_child();
                        let (i, _) = pending
                            .iter()
                            .enumerate()
                            .max_by_key(|(_, b)| **b)
                            .expect("an internal node has children");
                        let slice = node.buffer.drain(node.buffer_slice(i)).collect();
                        let rewrite = self.rewrite(node.children[i].ptr, slice).await?;
                        splice(&mut node, i, rewrite);
                    }
                    // Rewrite children that hold victims or droppable
                    // tombstones. Replacements need neither, so the scan steps
                    // past them. The child takes this node's messages for its
                    // range along: every rewrite carries its ancestors' messages
                    // down, so a rewritten leaf has no older version of its keys
                    // above it, and a tombstone there can go once no writer can
                    // still produce an older version.
                    let mut i = 0;
                    while i < node.children.len() {
                        let child = node.children[i];
                        if self.needs_move(child.ptr) || self.needs_purge(child.oldest_tombstone) {
                            let slice = node.buffer.drain(node.buffer_slice(i)).collect();
                            let rewrite = self.rewrite(child.ptr, slice).await?;
                            splice(&mut node, i, rewrite);
                        } else {
                            i += 1;
                        }
                    }
                    if node.children.is_empty() {
                        // Every child was flushed empty, so its buffer was too.
                        debug_assert!(node.buffer.is_empty());
                        Ok(Rewrite::Removed)
                    } else {
                        Ok(self.write_internals(node))
                    }
                }
            }
        })
    }

    fn write_child(&mut self, node: Node) -> Child {
        let oldest_tombstone = node.oldest_tombstone();
        Child {
            ptr: self.write(node),
            oldest_tombstone,
        }
    }

    fn write(&mut self, node: Node) -> NodePtr {
        let frame = codec::encode(codec::NODE, &node);
        let ptr = NodePtr {
            seg: self.seg,
            offset: self.buf.len() as u64,
            len: u32::try_from(frame.len()).expect("a node fits in 4 GiB"),
        };
        self.buf.extend_from_slice(&frame);
        self.written.push((ptr, node));
        ptr
    }

    fn write_leaves(&mut self, entries: Vec<(Bytes, Version)>) -> Rewrite {
        if entries.is_empty() {
            return Rewrite::Removed;
        }
        let chunks = if msgs_bytes(&entries) <= self.cfg.leaf_max_bytes {
            vec![entries]
        } else {
            chunk_by_bytes(entries, self.cfg.leaf_split_target(), |(k, v)| {
                entry_bytes(k, v.msg.encoded_len())
            })
        };
        let mut ids = chunks.into_iter().map(|entries| {
            let low = entries[0].0.clone();
            (low, Node::Leaf(Arc::new(Leaf { entries })))
        });
        let (_, first) = ids.next().expect("non-empty entries yield a chunk");
        let rest: Vec<_> = ids.collect();
        let first = self.write_child(first);
        let rest = rest
            .into_iter()
            .map(|(low, n)| (low, self.write_child(n)))
            .collect();
        Rewrite::Nodes { first, rest }
    }

    fn write_internals(&mut self, node: Internal) -> Rewrite {
        let n = node.children.len();
        let starts = group_starts(n, self.cfg.max_fanout);
        let ends = starts.iter().skip(1).copied().chain([n]);
        let groups: Vec<(Option<Bytes>, Internal)> = starts
            .iter()
            .zip(ends)
            .map(|(&a, b)| {
                let lo = a.checked_sub(1).map(|j| node.pivots[j].clone());
                let hi = node.pivots.get(b - 1).filter(|_| b < n);
                let buffer = node
                    .buffer
                    .iter()
                    .filter(|(k, _)| {
                        lo.as_ref().is_none_or(|lo| k >= lo) && hi.is_none_or(|hi| k < hi)
                    })
                    .cloned()
                    .collect();
                let group = Internal {
                    pivots: node.pivots[a..b - 1].to_vec(),
                    children: node.children[a..b].to_vec(),
                    buffer,
                };
                (lo, group)
            })
            .collect();
        let mut groups = groups.into_iter();
        let (_, first) = groups.next().expect("at least one group");
        let first = self.write_child(Node::Internal(Arc::new(first)));
        let rest = groups
            .map(|(lo, g)| {
                let lo = lo.expect("groups after the first have a lower pivot");
                (lo, self.write_child(Node::Internal(Arc::new(g))))
            })
            .collect();
        Rewrite::Nodes { first, rest }
    }
}

/// Replaces child `i` of `node` with the result of rewriting it.
fn splice(node: &mut Internal, i: usize, rewrite: Rewrite) {
    let replacement = match rewrite {
        Rewrite::Removed => None,
        Rewrite::Nodes { first, rest } => Some((first, rest)),
    };
    splice_children(&mut node.pivots, &mut node.children, i, replacement);
}

/// Replaces child `i` with `first` followed by `rest` (each tagged with the
/// pivot that goes before it), or removes it (`None`), handing its key range
/// to a neighbor. Keeps `children.len() == pivots.len() + 1`.
pub(crate) fn splice_children<P, C>(
    pivots: &mut Vec<P>,
    children: &mut Vec<C>,
    i: usize,
    replacement: Option<(C, Vec<(P, C)>)>,
) {
    match replacement {
        None => {
            children.remove(i);
            // Drop one boundary: the left neighbor's upper one, or for the
            // first child, the right neighbor's lower one.
            if !pivots.is_empty() {
                pivots.remove(i.saturating_sub(1));
            }
        }
        Some((first, rest)) => {
            children[i] = first;
            let (lows, more): (Vec<_>, Vec<_>) = rest.into_iter().unzip();
            children.splice(i + 1..i + 1, more);
            pivots.splice(i..i, lows);
        }
    }
}
