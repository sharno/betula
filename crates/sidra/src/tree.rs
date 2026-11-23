//! Bε-tree reads and copy-on-write batch application.
//!
//! An internal node buffers messages per child as append-only runs. A batch
//! arriving at a node is split by child and appended as new runs, so only the
//! node's small routing record is rewritten. When a node's runs exceed its
//! budget (or a child has too many runs), the child with the most pending
//! bytes is flushed: its runs are merged into it, recursively. Every node
//! touched gets a new pointer; untouched subtrees and runs are shared with the
//! previous version.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use bytes::{Bytes, BytesMut};

use crate::codec;
use crate::error::{Error, Result};
use crate::hlc::Ts;
use crate::node::{
    Child, Internal, Leaf, Node, Run, RunRef, apply_to_leaf, chunk_by_bytes, group_starts,
    merge_versions, msgs_bytes,
};
use crate::nodes::{Nodes, Segment};
use crate::types::{KeyRange, NodePtr, SegmentId, TreeRoot, Version, entry_bytes};

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct TreeConfig {
    /// Leaves larger than this are split.
    pub leaf_max_bytes: usize,
    /// Buffered bytes an internal node may hold across its children's runs
    /// before it flushes one child.
    pub internal_max_bytes: usize,
    /// Internal nodes with more children are split. This is the B^ε fanout.
    pub max_fanout: usize,
    /// A child with more buffered runs than this is flushed, which bounds
    /// the runs a read checks per level.
    pub max_runs_per_child: usize,
    /// A subtree is rewritten just to drop its tombstones only once they've
    /// been droppable this long (by timestamp). Younger ones go whenever
    /// their leaf is rewritten anyway; purging them at once would rewrite
    /// every leaf a delete reached a second time, a checkpoint later.
    #[serde(with = "humantime_serde")]
    pub purge_tombstones_after: std::time::Duration,
}

impl Default for TreeConfig {
    fn default() -> Self {
        Self {
            leaf_max_bytes: 512 << 10,
            internal_max_bytes: 4 << 20,
            max_fanout: 64,
            max_runs_per_child: 16,
            purge_tombstones_after: std::time::Duration::from_secs(60),
        }
    }
}

impl TreeConfig {
    fn leaf_split_target(&self) -> usize {
        self.leaf_max_bytes * 3 / 4
    }
}

/// A duration in hybrid-clock units (milliseconds above the logical bits).
fn hlc_span(d: std::time::Duration) -> u64 {
    let bits = crate::hlc::LOGICAL_BITS;
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX >> bits) << bits
}

fn corrupt(ptr: NodePtr, what: &str) -> Error {
    Error::Corrupt {
        path: ptr.to_string(),
        reason: format!("expected {what}"),
    }
}

/// The run behind `run`, resolved once per cached parent node.
async fn load_run(nodes: &Nodes, run: &RunRef) -> Result<Arc<Run>> {
    if let Some(r) = run.resolved.get() {
        return Ok(r.clone());
    }
    match nodes.load(run.ptr).await? {
        Node::Run(r) => Ok(run.resolved.get_or_init(|| r).clone()),
        Node::Leaf(_) | Node::Internal(_) => Err(corrupt(run.ptr, "a run")),
    }
}

/// Whether `best` already beats everything with versions at most `bound`.
fn beats(best: &Option<Version>, bound: Option<Ts>) -> bool {
    best.as_ref()
        .is_some_and(|b| bound.is_none_or(|n| b.ts > n))
}

/// The newest version of `key` in the tree, given the newest found so far
/// above it (`best`). A late write from a slow writer can sit above a newer
/// version, so the walk continues down until `best` beats the newest version
/// of the subtree below, which child pointers record.
pub async fn get(
    nodes: &Nodes,
    root: Option<TreeRoot>,
    key: &[u8],
    mut best: Option<Version>,
) -> Result<Option<Version>> {
    let Some(root) = root else { return Ok(best) };
    if beats(&best, root.newest) {
        return Ok(best);
    }
    let mut ptr = root.ptr;
    loop {
        match nodes.load(ptr).await? {
            Node::Leaf(leaf) => return Ok(Version::newest(best, leaf.get(key).cloned())),
            Node::Run(_) => return Err(corrupt(ptr, "a tree node")),
            Node::Internal(node) => {
                let child = &node.children[node.child_index(key)];
                for run in child.runs.iter().rev() {
                    if !beats(&best, Some(run.newest)) {
                        let r = load_run(nodes, run).await?;
                        best = Version::newest(best, r.get(key).cloned());
                    }
                }
                if beats(&best, child.newest) {
                    return Ok(best);
                }
                ptr = child.ptr;
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TreeShape {
    pub internal: usize,
    pub leaves: usize,
    pub runs: usize,
    /// Tombstones still held in leaves.
    pub tombstones: usize,
    /// Every reachable node and run.
    pub nodes: Vec<NodePtr>,
}

/// Walks the whole tree. Diagnostic: loads every leaf.
pub async fn shape(nodes: &Nodes, root: Option<TreeRoot>) -> Result<TreeShape> {
    let mut shape = TreeShape::default();
    let mut level: Vec<NodePtr> = root.map(|r| r.ptr).into_iter().collect();
    while !level.is_empty() {
        let loaded = nodes.load_many(&level).await?;
        let mut next = Vec::new();
        for (ptr, node) in level.iter().zip(&loaded) {
            shape.nodes.push(*ptr);
            match node {
                Node::Internal(i) => {
                    shape.internal += 1;
                    shape.nodes.extend(i.run_ptrs());
                    shape.runs += i.run_ptrs().count();
                    next.extend(i.children.iter().map(|c| c.ptr));
                }
                Node::Leaf(l) => {
                    shape.leaves += 1;
                    shape.tombstones += l
                        .entries
                        .iter()
                        .filter(|(_, v)| v.value().is_none())
                        .count();
                }
                Node::Run(_) => return Err(corrupt(*ptr, "a tree node")),
            }
        }
        level = next;
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
    /// Inside a leaf, resuming after `after` (from its start if `None`).
    InLeaf {
        stack: Vec<Frame>,
        leaf: Arc<Leaf>,
        after: Option<Bytes>,
    },
    Done,
}

/// Keys built per chunk. Short scans then clone about what they return:
/// cloning `Bytes` is an atomic increment, contended when many readers scan
/// the same leaves.
const PAGE: usize = 64;

/// Visits leaves in key order. Each leaf's view is its entries overlaid with
/// the runs buffered above it on its path, newest version per key winning.
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

    /// The next non-empty run of versions (tombstones included, so they can
    /// shadow log tails), in key order.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<(Bytes, Version)>>> {
        loop {
            let (mut stack, leaf, after) = match std::mem::replace(&mut self.state, IterState::Done)
            {
                IterState::Done | IterState::Start(None) => return Ok(None),
                IterState::Start(Some(root)) => {
                    let mut stack = Vec::new();
                    let leaf = self.descend(root.ptr, &mut stack).await?;
                    (stack, leaf, None)
                }
                IterState::InLeaf { stack, leaf, after } => (stack, leaf, after),
            };
            let (chunk, more) = self.materialize(&stack, &leaf, after.as_ref()).await?;
            self.state = match (more, chunk.last()) {
                (true, Some((last, _))) => IterState::InLeaf {
                    after: Some(last.clone()),
                    stack,
                    leaf,
                },
                _ => match self.advance(&mut stack).await? {
                    Some(leaf) => IterState::InLeaf {
                        stack,
                        leaf,
                        after: None,
                    },
                    None => IterState::Done,
                },
            };
            if !chunk.is_empty() {
                return Ok(Some(chunk));
            }
        }
    }

    async fn descend(&self, mut ptr: NodePtr, stack: &mut Vec<Frame>) -> Result<Arc<Leaf>> {
        loop {
            match self.nodes.load(ptr).await? {
                Node::Leaf(leaf) => return Ok(leaf),
                Node::Run(_) => return Err(corrupt(ptr, "a tree node")),
                Node::Internal(node) => {
                    let idx = node.first_child(&self.range.start);
                    let last = node.last_child(&self.range.end).max(idx);
                    ptr = node.children[idx].ptr;
                    stack.push(Frame { node, idx, last });
                }
            }
        }
    }

    async fn advance(&self, stack: &mut Vec<Frame>) -> Result<Option<Arc<Leaf>>> {
        while let Some(top) = stack.last_mut() {
            if top.idx < top.last {
                top.idx += 1;
                let ptr = top.node.children[top.idx].ptr;
                return self.descend(ptr, stack).await.map(Some);
            }
            stack.pop();
        }
        Ok(None)
    }

    /// Up to `PAGE` keys of the leaf's view after `after`, and whether the
    /// leaf has more. The view is the leaf overlaid with the runs above it on
    /// its path, newest version per key.
    async fn materialize(
        &self,
        stack: &[Frame],
        leaf: &Leaf,
        after: Option<&Bytes>,
    ) -> Result<(Vec<(Bytes, Version)>, bool)> {
        // The leaf's key range is the intersection of every ancestor's child
        // bounds; a frame's own bound is `None` where it inherits its parent's.
        let (lo, hi) = stack.iter().fold((None, None), |(lo, hi), f| {
            let (flo, fhi) = f.node.child_bounds(f.idx);
            (flo.or(lo), fhi.or(hi))
        });
        let wanted = |k: &Bytes| after.is_none_or(|a| k > a) && self.range.contains(k);
        // Each source is sorted: take at most PAGE wanted entries from each.
        // The first PAGE keys of the union are complete up to the smallest
        // last key among truncated sources.
        let mut candidates: Vec<(Bytes, Version)> = Vec::new();
        let mut cutoff: Option<Bytes> = None;
        let mut take = |entries: &[(Bytes, Version)], candidates: &mut Vec<(Bytes, Version)>| {
            let start = match (after, lo) {
                (Some(a), _) => entries.partition_point(|(k, _)| k <= a),
                (None, Some(lo)) => entries.partition_point(|(k, _)| k < lo),
                (None, None) => 0,
            };
            let end = hi.map_or(entries.len(), |hi: &Bytes| {
                entries.partition_point(|(k, _)| k < hi)
            });
            let mut wanted_entries = entries[start..end.max(start)]
                .iter()
                .filter(|(k, _)| wanted(k));
            candidates.extend(wanted_entries.by_ref().take(PAGE).cloned());
            if wanted_entries.next().is_some() {
                let last = &candidates[candidates.len() - 1].0;
                if cutoff.as_ref().is_none_or(|c| last < c) {
                    cutoff = Some(last.clone());
                }
            }
        };
        take(&leaf.entries, &mut candidates);
        let from_leaf = candidates.len();
        for f in stack {
            for run in &f.node.children[f.idx].runs {
                let r = load_run(&self.nodes, run).await?;
                take(&r.entries, &mut candidates);
            }
        }
        if candidates.len() > from_leaf {
            candidates.sort_unstable_by(|(ka, va), (kb, vb)| ka.cmp(kb).then(vb.ts.cmp(&va.ts)));
            candidates.dedup_by(|later, first| later.0 == first.0);
        }
        let more = match &cutoff {
            Some(c) => {
                candidates.retain(|(k, _)| k <= c);
                true
            }
            None => false,
        };
        Ok((candidates, more))
    }
}

// ---------------------------------------------------------------------------
// Copy-on-write application

/// Result of rewriting a subtree.
enum Rewrite {
    /// The subtree became empty.
    Removed,
    /// The subtree is now `first` followed by `rest`, each tagged with its
    /// lowest key (the pivot to insert before it). New children carry no
    /// runs: whatever was buffered for the old child went into it.
    Nodes {
        first: Child,
        rest: Vec<(Bytes, Child)>,
    },
}

/// Where a checkpoint writes. Inner nodes and runs go to one segment and
/// leaves to another: warming inner nodes is then one contiguous read per
/// checkpoint, and short-lived inner data doesn't keep leaf segments alive.
#[derive(Clone, Copy, Debug)]
pub struct Outputs {
    pub inner: SegmentId,
    pub leaves: SegmentId,
}

struct Region {
    id: SegmentId,
    buf: BytesMut,
    written: Vec<(NodePtr, Node)>,
}

impl Region {
    fn new(id: SegmentId) -> Self {
        Self {
            id,
            buf: BytesMut::new(),
            written: Vec::new(),
        }
    }

    fn into_segment(self) -> Option<Segment> {
        (!self.written.is_empty()).then(|| Segment {
            id: self.id,
            data: self.buf.freeze(),
            nodes: self.written,
        })
    }
}

/// A new tree version, not yet uploaded or published.
pub struct Plan {
    pub root: Option<TreeRoot>,
    /// New segments (none if nothing changed).
    pub segments: Vec<Segment>,
    /// Nodes and runs of the previous version that the new version no longer
    /// uses.
    pub freed: Vec<NodePtr>,
    pub stats: WriteStats,
}

/// Where a checkpoint's bytes went.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteStats {
    /// Encoded bytes by node kind.
    pub run_bytes: u64,
    pub internal_bytes: u64,
    pub leaf_bytes: u64,
    /// Runs copied in place out of victim segments (part of `run_bytes`).
    pub relocated_run_bytes: u64,
    /// Run bytes by the depth of the node holding them (0: root; the last
    /// slot collects deeper ones).
    pub run_bytes_by_depth: [u64; 4],
    /// Leaves rewritten, and the messages applied to them: few messages per
    /// leaf means leaf rewrites dominate write amplification.
    pub leaves_rewritten: u64,
    pub leaf_msgs: u64,
    /// Leaves rewritten with no messages: moved by the cleaner or purged of
    /// tombstones.
    pub leaves_moved: u64,
}

/// Applies sorted, key-unique `msgs` to the tree at `root`, writing new nodes
/// into `out`. Live nodes and runs in `victims` segments are rewritten
/// too, so those segments can be deleted once nothing else references them.
/// How one checkpoint writes: where to, what to relocate, which tombstones
/// may go, and how to encode.
pub struct Checkpoint<'a> {
    pub out: Outputs,
    /// Segments whose live nodes and runs are copied out.
    pub victims: &'a HashSet<SegmentId>,
    /// Tombstones older than this are dropped when their leaf is rewritten.
    pub drop_tombstones_below: Ts,
    pub compression: codec::Compression,
}

pub async fn apply(
    nodes: &Nodes,
    cfg: &TreeConfig,
    ck: Checkpoint<'_>,
    root: Option<TreeRoot>,
    msgs: Vec<(Bytes, Version)>,
) -> Result<Plan> {
    let Checkpoint {
        out,
        victims,
        drop_tombstones_below,
        compression,
    } = ck;
    let mut w = Writer {
        nodes,
        cfg,
        compression,
        victims,
        drop_below: drop_tombstones_below,
        purge_below: Ts {
            hlc: drop_tombstones_below
                .hlc
                .saturating_sub(hlc_span(cfg.purge_tombstones_after)),
            ..drop_tombstones_below
        },
        dirty: HashSet::new(),
        inner: Region::new(out.inner),
        leaves: Region::new(out.leaves),
        freed: Vec::new(),
        stats: WriteStats::default(),
        depth: 0,
    };
    if let Some(r) = root.filter(|_| !victims.is_empty()) {
        w.mark_dirty(r.ptr, r.height).await?;
    }
    let root = w.apply_root(root, msgs).await?;
    Ok(Plan {
        root,
        segments: [w.inner, w.leaves]
            .into_iter()
            .filter_map(Region::into_segment)
            .collect(),
        freed: w.freed,
        stats: w.stats,
    })
}

struct Writer<'a> {
    nodes: &'a Nodes,
    cfg: &'a TreeConfig,
    compression: codec::Compression,
    victims: &'a HashSet<SegmentId>,
    /// Tombstones older than this are dropped when their leaf is rewritten.
    drop_below: Ts,
    /// Subtrees whose oldest tombstone is below this get rewritten to drop it.
    purge_below: Ts,
    /// Internal nodes with a victim node or run at or below them.
    dirty: HashSet<NodePtr>,
    inner: Region,
    leaves: Region,
    freed: Vec<NodePtr>,
    stats: WriteStats,
    /// Depth of the node being rewritten (0: root).
    depth: usize,
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// New messages for a node, split by child.
type Parts = Vec<Vec<(Bytes, Version)>>;

fn split_by_child(pivots: &[Bytes], msgs: Vec<(Bytes, Version)>) -> Parts {
    let mut parts: Parts = vec![Vec::new(); pivots.len() + 1];
    let mut child = 0;
    for (k, v) in msgs {
        while child < pivots.len() && k >= pivots[child] {
            child += 1;
        }
        parts[child].push((k, v));
    }
    parts
}

impl Writer<'_> {
    fn needs_purge(&self, child: &Child) -> bool {
        child.oldest_tombstone.is_some_and(|t| t < self.purge_below)
    }

    /// Whether the child or its subtree holds a node that must move out of a
    /// victim segment.
    fn subtree_needs_move(&self, child: &Child) -> bool {
        self.victims.contains(&child.ptr.seg) || self.dirty.contains(&child.ptr)
    }

    fn runs_need_move(&self, child: &Child) -> bool {
        child.runs.iter().any(|r| self.victims.contains(&r.ptr.seg))
    }

    /// Copies the child's runs that live in victim segments. Unlike moving a
    /// node, this needn't push them down: they stay buffered where they are.
    async fn relocate_runs(&mut self, child: &mut Child) -> Result<()> {
        for run in child.runs.iter_mut() {
            if self.victims.contains(&run.ptr.seg) {
                let r = load_run(self.nodes, run).await?;
                self.freed.push(run.ptr);
                *run = self.write_run(r.entries.clone());
                self.stats.relocated_run_bytes += u64::from(run.ptr.len);
            }
        }
        Ok(())
    }

    /// Finds internal nodes on paths to victim nodes or runs. Walks only
    /// internal nodes (cached); leaves and runs are judged by the pointers
    /// their parent holds.
    fn mark_dirty(&mut self, ptr: NodePtr, height: u32) -> BoxFut<'_, bool> {
        Box::pin(async move {
            let in_victim = self.victims.contains(&ptr.seg);
            if height == 0 {
                return Ok(in_victim);
            }
            let Node::Internal(node) = self.nodes.load(ptr).await? else {
                return Err(corrupt(ptr, "an internal node"));
            };
            let mut dirty = in_victim;
            for child in &node.children {
                let below = self.mark_dirty(child.ptr, height - 1).await?;
                dirty |= below || child.runs.iter().any(|r| self.victims.contains(&r.ptr.seg));
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
                    && !self.victims.contains(&r.ptr.seg)
                    && !self.dirty.contains(&r.ptr)
                    && !r.oldest_tombstone.is_some_and(|t| t < self.purge_below) =>
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
                        newest: first.newest,
                        height,
                    };
                }
                Rewrite::Nodes { first, rest } => {
                    let (pivots, more): (Vec<_>, Vec<_>) = rest.into_iter().unzip();
                    let children = std::iter::once(first).chain(more).collect();
                    level = self.write_internals(Internal { pivots, children });
                    height += 1;
                }
            }
        };
        // Shrink: a root with one child and nothing buffered is just a hop.
        // The root is always the inner node written last, so unwrite it.
        while let Some((ptr, Node::Internal(node))) = self.inner.written.last() {
            if *ptr != root.ptr || node.children.len() != 1 || !node.children[0].runs.is_empty() {
                break;
            }
            let only = &node.children[0];
            root = TreeRoot {
                ptr: only.ptr,
                oldest_tombstone: only.oldest_tombstone,
                newest: only.newest,
                height: root.height - 1,
            };
            self.inner.buf.truncate(ptr.offset as usize);
            self.inner.written.pop();
        }
        Ok(Some(root))
    }

    /// Rewrites the node at `ptr` with `msgs` applied, moving any victim
    /// nodes below it and dropping tombstones that became droppable.
    fn rewrite(&mut self, ptr: NodePtr, msgs: Vec<(Bytes, Version)>) -> BoxFut<'_, Rewrite> {
        Box::pin(async move {
            debug_assert!(!self.is_ours(ptr), "rewrite of a node from this checkpoint");
            let node = self.nodes.load(ptr).await?;
            self.freed.push(ptr);
            match node {
                Node::Run(_) => Err(corrupt(ptr, "a tree node")),
                Node::Leaf(leaf) => {
                    self.stats.leaves_rewritten += 1;
                    self.stats.leaf_msgs += msgs.len() as u64;
                    self.stats.leaves_moved += u64::from(msgs.is_empty());
                    Ok(self.write_leaves(apply_to_leaf(
                        leaf.entries.clone(),
                        msgs,
                        self.drop_below,
                    )))
                }
                Node::Internal(old) => {
                    let mut node = old.as_ref().clone();
                    let mut parts = split_by_child(&node.pivots, msgs);
                    // Flush children while over budget. Choosing before
                    // writing runs means a child that is about to be flushed
                    // gets its new messages directly, without a run.
                    while let Some(i) = self.pick_flush(&node, &parts) {
                        self.flush_child(&mut node, &mut parts, i).await?;
                    }
                    // Rewrite children whose subtree holds victims or droppable
                    // tombstones. Replacements need neither, so the scan steps
                    // past them. Their runs and new messages come along: every
                    // rewrite carries the buffered versions above a child down
                    // into it, so a rewritten leaf has no older version of its
                    // keys above it, and a tombstone there can go once no
                    // writer can still produce an older version. Runs that
                    // merely sit in a victim segment are copied in place.
                    let mut i = 0;
                    while i < node.children.len() {
                        let child = &node.children[i];
                        if self.subtree_needs_move(child) || self.needs_purge(child) {
                            self.flush_child(&mut node, &mut parts, i).await?;
                        } else {
                            if self.runs_need_move(child) {
                                let mut child = node.children[i].clone();
                                self.relocate_runs(&mut child).await?;
                                node.children[i] = child;
                            }
                            i += 1;
                        }
                    }
                    // Only now write the remaining new messages as runs, so a
                    // checkpoint never reads back a run it wrote itself (its
                    // segment isn't uploaded yet).
                    for (child, part) in node.children.iter_mut().zip(parts.iter_mut()) {
                        if !part.is_empty() {
                            let run = self.write_run(std::mem::take(part));
                            child.runs.push(run);
                        }
                    }
                    if node.children.is_empty() {
                        Ok(Rewrite::Removed)
                    } else {
                        Ok(self.write_internals(node))
                    }
                }
            }
        })
    }

    /// The child to flush next: one that would exceed the run limit, else the
    /// one with the most pending bytes when the node is over budget.
    fn pick_flush(&self, node: &Internal, parts: &Parts) -> Option<usize> {
        let pending: Vec<usize> = node
            .children
            .iter()
            .zip(parts)
            .map(|(c, p)| c.pending_bytes() + msgs_bytes(p))
            .collect();
        let too_many_runs = node.children.iter().zip(parts).position(|(c, p)| {
            c.runs.len() + usize::from(!p.is_empty()) > self.cfg.max_runs_per_child
        });
        let over_budget = pending.iter().sum::<usize>() > self.cfg.internal_max_bytes;
        too_many_runs.or_else(|| {
            over_budget
                .then(|| {
                    pending
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, b)| **b)
                        .map(|(i, _)| i)
                })
                .flatten()
        })
    }

    /// Merges child `i`'s runs and new messages into it.
    async fn flush_child(
        &mut self,
        node: &mut Internal,
        parts: &mut Parts,
        i: usize,
    ) -> Result<()> {
        let child = node.children[i].clone();
        let run_ptrs: Vec<NodePtr> = child.runs.iter().map(|r| r.ptr).collect();
        // Runs of this checkpoint live in its unuploaded segment; the run
        // ordering in `rewrite` guarantees none is read back.
        debug_assert!(
            run_ptrs.iter().all(|p| !self.is_ours(*p)),
            "run from this checkpoint"
        );
        let runs = self.nodes.load_many(&run_ptrs).await?;
        let mut msgs = std::mem::take(&mut parts[i]);
        for (ptr, run) in run_ptrs.iter().zip(runs) {
            let Node::Run(run) = run else {
                return Err(corrupt(*ptr, "a run"));
            };
            msgs = merge_versions(msgs, run.entries.clone());
        }
        self.freed.extend(run_ptrs);
        self.depth += 1;
        let rewrite = self.rewrite(child.ptr, msgs).await;
        self.depth -= 1;
        let rewrite = rewrite?;
        let replacement = match rewrite {
            Rewrite::Removed => None,
            Rewrite::Nodes { first, rest } => Some((first, rest)),
        };
        match &replacement {
            None => {
                parts.remove(i);
            }
            Some((_, rest)) => {
                parts.splice(i + 1..i + 1, rest.iter().map(|_| Vec::new()));
            }
        }
        splice_children(&mut node.pivots, &mut node.children, i, replacement);
        Ok(())
    }

    fn write_run(&mut self, entries: Vec<(Bytes, Version)>) -> RunRef {
        let bytes = u32::try_from(msgs_bytes(&entries)).unwrap_or(u32::MAX);
        let newest = entries
            .iter()
            .map(|(_, v)| v.ts)
            .max()
            .expect("runs are written only for non-empty parts");
        let run = Arc::new(Run { entries });
        RunRef {
            ptr: self.write(Node::Run(run.clone())),
            bytes,
            newest,
            resolved: OnceLock::from(run),
        }
    }

    fn write_child(&mut self, node: Node) -> Child {
        let oldest_tombstone = node.oldest_tombstone();
        let newest = node.newest();
        Child {
            ptr: self.write(node),
            oldest_tombstone,
            newest,
            runs: Vec::new(),
        }
    }

    fn is_ours(&self, ptr: NodePtr) -> bool {
        ptr.seg == self.inner.id || ptr.seg == self.leaves.id
    }

    fn write(&mut self, node: Node) -> NodePtr {
        let frame = codec::encode(codec::NODE, &node, self.compression);
        let n = frame.len() as u64;
        let region = match node {
            Node::Leaf(_) => {
                self.stats.leaf_bytes += n;
                &mut self.leaves
            }
            Node::Internal(_) => {
                self.stats.internal_bytes += n;
                &mut self.inner
            }
            Node::Run(_) => {
                self.stats.run_bytes += n;
                self.stats.run_bytes_by_depth[self.depth.min(3)] += n;
                &mut self.inner
            }
        };
        let ptr = NodePtr {
            seg: region.id,
            offset: region.buf.len() as u64,
            len: u32::try_from(frame.len()).expect("a node fits in 4 GiB"),
        };
        region.buf.extend_from_slice(&frame);
        region.written.push((ptr, node));
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
        let mut leaves = chunks.into_iter().map(|entries| {
            let low = entries[0].0.clone();
            (low, Node::Leaf(Arc::new(Leaf { entries })))
        });
        let (_, first) = leaves.next().expect("non-empty entries yield a chunk");
        let rest: Vec<_> = leaves.collect();
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
                let group = Internal {
                    pivots: node.pivots[a..b - 1].to_vec(),
                    children: node.children[a..b].to_vec(),
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
