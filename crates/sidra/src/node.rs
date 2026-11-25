use std::ops::Bound;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::hlc::Ts;
use crate::types::{Msg, NodePtr, Version, entry_bytes};

/// Tree node. Nodes are immutable once written: every change produces a new
/// node under a new id (copy-on-write).
/// Variants are `Arc`ed so cache hits and iterator frames are cheap clones.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Node {
    Leaf(Arc<Leaf>),
    Internal(Arc<Internal>),
    /// A buffered run of messages for one child of an internal node.
    Run(Arc<Run>),
}

/// Sorted, one version per key, no tombstones.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leaf {
    pub entries: Vec<(Bytes, Version)>,
}

/// Sorted, one version per key: messages buffered above a child.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub entries: Vec<(Bytes, Version)>,
}

/// `children[i]` covers keys in `[pivots[i-1], pivots[i])`, so
/// `children.len() == pivots.len() + 1`. The Bε buffer is kept per child as
/// append-only runs (`Child::runs`): adding messages writes new runs and
/// rewrites only this small routing node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Internal {
    pub pivots: Vec<Bytes>,
    pub children: Vec<Child>,
}

/// A child pointer, the runs buffered above that child, and summaries of the
/// subtree that let checkpoints and reads skip it without loading it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Child {
    pub ptr: NodePtr,
    /// When the subtree below `runs` first holds tombstones worth purging
    /// (`Node::purge_at`), so checkpoints find them without loading it.
    pub purge_at: Option<Ts>,
    /// Newest version in the subtree (excluding `runs`): a read that already
    /// found something newer stops here.
    pub newest: Option<Ts>,
    /// Messages buffered for this child, newest run last.
    pub runs: Vec<RunRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRef {
    pub ptr: NodePtr,
    /// Logical message bytes, for flush decisions without loading the run.
    pub bytes: u32,
    pub newest: Ts,
    /// Whether the run holds a tombstone.
    pub tombstones: bool,
    /// The run once loaded. Internal nodes are immutable and shared through
    /// the cache, so a read resolves each run once and later reads skip the
    /// cache lookup. Runs stay alive as long as their parent node does.
    #[serde(skip)]
    pub resolved: OnceLock<Arc<Run>>,
}

impl PartialEq for RunRef {
    fn eq(&self, other: &Self) -> bool {
        (self.ptr, self.bytes, self.newest, self.tombstones)
            == (other.ptr, other.bytes, other.newest, other.tombstones)
    }
}

impl Eq for RunRef {}

impl Child {
    pub fn pending_bytes(&self) -> usize {
        self.runs.iter().map(|r| r.bytes as usize).sum()
    }

    /// `purge_at` including this child's own runs.
    pub fn purge_at_with_runs(&self) -> Option<Ts> {
        buffer_purge_at(self.runs.iter().map(|r| (r.newest, r.tombstones)))
            .into_iter()
            .chain(self.purge_at)
            .min()
    }

    /// Newest version in the subtree including the buffered runs.
    pub fn newest_with_runs(&self) -> Option<Ts> {
        self.runs.iter().map(|r| r.newest).chain(self.newest).max()
    }
}

/// When tombstones in a buffer of runs, each given as (newest version,
/// holds a tombstone), are worth purging: once the buffer has gone quiet,
/// i.e. at its newest version. A buffer that's still filling flushes on its
/// own; purging it early would only cost its batching.
pub fn buffer_purge_at(runs: impl Iterator<Item = (Ts, bool)>) -> Option<Ts> {
    let (newest, tombstones) = runs.fold((None, false), |(n, t), (newest, tomb)| {
        (n.max(Some(newest)), t || tomb)
    });
    newest.filter(|_| tombstones)
}

fn newest_in(entries: &[(Bytes, Version)]) -> Option<Ts> {
    entries.iter().map(|(_, v)| v.ts).max()
}

impl Node {
    /// When a checkpoint should flush this subtree's buffers just to drop
    /// their tombstones (once this is `purge_tombstones_after` old): the
    /// newest version of a buffer holding tombstones (`buffer_purge_at`).
    /// Buffered tombstones must be carried down into their leaf to go, or a
    /// range nobody writes to again keeps them and the values they delete
    /// forever.
    pub fn purge_at(&self) -> Option<Ts> {
        match self {
            // Leaves hold no tombstones (`apply_to_leaf`).
            Node::Leaf(_) => None,
            Node::Internal(i) => i
                .children
                .iter()
                .filter_map(Child::purge_at_with_runs)
                .min(),
            // Summarized by the parent's `RunRef`.
            Node::Run(_) => None,
        }
    }

    pub fn newest(&self) -> Option<Ts> {
        match self {
            Node::Leaf(l) => newest_in(&l.entries),
            Node::Run(r) => newest_in(&r.entries),
            Node::Internal(i) => i.children.iter().filter_map(Child::newest_with_runs).max(),
        }
    }
}

fn lookup<'a>(entries: &'a [(Bytes, Version)], key: &[u8]) -> Option<&'a Version> {
    entries
        .binary_search_by(|(k, _)| k.as_ref().cmp(key))
        .ok()
        .map(|i| &entries[i].1)
}

impl Leaf {
    pub fn get(&self, key: &[u8]) -> Option<&Version> {
        lookup(&self.entries, key)
    }
}

impl Run {
    pub fn get(&self, key: &[u8]) -> Option<&Version> {
        lookup(&self.entries, key)
    }
}

impl Internal {
    pub fn child_index(&self, key: &[u8]) -> usize {
        child_index::<[u8], _>(&self.pivots, key)
    }

    /// First child that can hold keys at or after `start`.
    pub fn first_child(&self, start: &Bound<Bytes>) -> usize {
        first_child::<[u8], _>(&self.pivots, start)
    }

    /// Last child that can hold keys at or before `end`.
    pub fn last_child(&self, end: &Bound<Bytes>) -> usize {
        last_child::<[u8], _>(&self.pivots, end)
    }

    /// Key bounds of child `i`.
    pub fn child_bounds(&self, i: usize) -> (Option<&Bytes>, Option<&Bytes>) {
        child_bounds(&self.pivots, i)
    }

    /// Every run referenced by this node.
    pub fn run_ptrs(&self) -> impl Iterator<Item = NodePtr> + '_ {
        self.children
            .iter()
            .flat_map(|c| c.runs.iter().map(|r| r.ptr))
    }
}

pub fn msgs_bytes(msgs: &[(Bytes, Version)]) -> usize {
    msgs.iter()
        .map(|(k, v)| entry_bytes(k, v.msg.encoded_len()))
        .sum()
}

// Routing over sorted pivots, generic so it can be verified with small types.
// Child `i` covers `[pivots[i-1], pivots[i])`.

pub fn child_index<K: Ord + ?Sized, P: std::borrow::Borrow<K>>(pivots: &[P], key: &K) -> usize {
    pivots.partition_point(|p| p.borrow() <= key)
}

pub fn first_child<K: Ord + ?Sized, P: std::borrow::Borrow<K>>(
    pivots: &[P],
    start: &Bound<P>,
) -> usize {
    match start {
        Bound::Included(k) | Bound::Excluded(k) => child_index::<K, P>(pivots, k.borrow()),
        Bound::Unbounded => 0,
    }
}

pub fn last_child<K: Ord + ?Sized, P: std::borrow::Borrow<K>>(
    pivots: &[P],
    end: &Bound<P>,
) -> usize {
    match end {
        Bound::Included(k) => child_index::<K, P>(pivots, k.borrow()),
        // Keys strictly below a pivot never land in the pivot's child.
        Bound::Excluded(k) => pivots.partition_point(|p| p.borrow() < k.borrow()),
        Bound::Unbounded => pivots.len(),
    }
}

pub fn child_bounds<P>(pivots: &[P], i: usize) -> (Option<&P>, Option<&P>) {
    let lo = i.checked_sub(1).map(|j| &pivots[j]);
    (lo, pivots.get(i))
}

/// Merges two sorted, key-unique runs, keeping the newer version per key.
pub fn merge_versions(a: Vec<(Bytes, Version)>, b: Vec<(Bytes, Version)>) -> Vec<(Bytes, Version)> {
    merge_newest(a, b, |v| v.ts)
}

/// Applies sorted, key-unique versions to leaf entries. Tombstones go: a
/// leaf has nothing below it to shadow, and every later write is newer.
pub fn apply_to_leaf(
    entries: Vec<(Bytes, Version)>,
    msgs: Vec<(Bytes, Version)>,
) -> Vec<(Bytes, Version)> {
    merge_newest(entries, msgs, |v| v.ts)
        .into_iter()
        .filter(|(_, v)| v.msg != Msg::Delete)
        .collect()
}

/// Merges two sorted, key-unique runs. On equal keys the entry with the
/// larger stamp wins (`a` on a tie).
pub fn merge_newest<K: Ord, V, T: Ord>(
    a: Vec<(K, V)>,
    b: Vec<(K, V)>,
    stamp: impl Fn(&V) -> T,
) -> Vec<(K, V)> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
    loop {
        let next = match (a.peek(), b.peek()) {
            (None, None) => break,
            (Some(_), None) => a.next(),
            (None, Some(_)) => b.next(),
            (Some((ka, _)), Some((kb, _))) if ka < kb => a.next(),
            (Some((ka, _)), Some((kb, _))) if kb < ka => b.next(),
            (Some(_), Some(_)) => {
                let (k, va) = a.next().expect("peeked");
                let (_, vb) = b.next().expect("peeked");
                Some((k, if stamp(&vb) > stamp(&va) { vb } else { va }))
            }
        };
        out.extend(next);
    }
    out
}

/// Splits `items` into the fewest consecutive chunks of roughly equal size
/// such that each chunk is about `target` bytes or less. Balancing avoids a
/// tiny trailing chunk.
pub fn chunk_by_bytes<T>(items: Vec<T>, target: usize, size: impl Fn(&T) -> usize) -> Vec<Vec<T>> {
    let total: usize = items.iter().map(&size).sum();
    let n = total.div_ceil(target.max(1)).max(1);
    let per_chunk = total.div_ceil(n);
    items
        .into_iter()
        .fold(Vec::new(), |mut chunks: Vec<(usize, Vec<T>)>, item| {
            let s = size(&item);
            match chunks.last_mut() {
                Some((bytes, chunk)) if *bytes < per_chunk => {
                    *bytes += s;
                    chunk.push(item);
                }
                _ => chunks.push((s, vec![item])),
            }
            chunks
        })
        .into_iter()
        .map(|(_, c)| c)
        .collect()
}

/// Splits `n` children into the fewest groups of at most `max` each, balanced.
/// Returns group start indices.
pub fn group_starts(n: usize, max: usize) -> Vec<usize> {
    let groups = n.div_ceil(max).max(1);
    (0..groups).map(|g| g * n / groups).collect()
}
