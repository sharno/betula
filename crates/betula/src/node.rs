use std::ops::Bound;
use std::sync::Arc;

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
}

/// Sorted, one version per key. Tombstones stay until no older write can
/// still arrive.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leaf {
    pub entries: Vec<(Bytes, Version)>,
}

/// `children[i]` covers keys in `[pivots[i-1], pivots[i])`, so
/// `children.len() == pivots.len() + 1`. `buffer` is sorted and holds at most
/// one version per key. A buffered version is not necessarily newer than one
/// below it (a late write from a slow writer), so reads compare timestamps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Internal {
    pub pivots: Vec<Bytes>,
    pub children: Vec<Child>,
    pub buffer: Vec<(Bytes, Version)>,
}

/// A child pointer plus the oldest tombstone in that subtree's leaves, so a
/// checkpoint can find leaves whose tombstones became droppable without
/// loading any leaf.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Child {
    pub ptr: NodePtr,
    pub oldest_tombstone: Option<Ts>,
}

impl Node {
    pub fn oldest_tombstone(&self) -> Option<Ts> {
        match self {
            Node::Leaf(l) => l
                .entries
                .iter()
                .filter(|(_, v)| v.msg == Msg::Delete)
                .map(|(_, v)| v.ts)
                .min(),
            Node::Internal(i) => i.children.iter().filter_map(|c| c.oldest_tombstone).min(),
        }
    }
}

impl Leaf {
    pub fn get(&self, key: &[u8]) -> Option<&Version> {
        self.entries
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }
}

impl Internal {
    pub fn child_index(&self, key: &[u8]) -> usize {
        child_index::<[u8], _>(&self.pivots, key)
    }

    pub fn buffered(&self, key: &[u8]) -> Option<&Version> {
        self.buffer
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|i| &self.buffer[i].1)
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

    /// Index range of `buffer` destined for child `i`.
    pub fn buffer_slice(&self, i: usize) -> std::ops::Range<usize> {
        let (lo, hi) = self.child_bounds(i);
        let start = lo.map_or(0, |lo| self.buffer.partition_point(|(k, _)| k < lo));
        let end = hi.map_or(self.buffer.len(), |hi| {
            self.buffer.partition_point(|(k, _)| k < hi)
        });
        start..end
    }

    pub fn buffer_bytes(&self) -> usize {
        msgs_bytes(&self.buffer)
    }

    pub fn routing_bytes(&self) -> usize {
        self.pivots.iter().map(|p| p.len() + 8).sum::<usize>() + self.children.len() * 56
    }

    /// Buffered bytes per child, in child order.
    pub fn pending_per_child(&self) -> Vec<usize> {
        (0..self.children.len())
            .map(|i| msgs_bytes(&self.buffer[self.buffer_slice(i)]))
            .collect()
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

/// Applies sorted, key-unique versions to leaf entries. Tombstones older than
/// `drop_below` are removed: by then no older write for their key can arrive.
pub fn apply_to_leaf(
    entries: Vec<(Bytes, Version)>,
    msgs: Vec<(Bytes, Version)>,
    drop_below: Ts,
) -> Vec<(Bytes, Version)> {
    merge_newest(entries, msgs, |v| v.ts)
        .into_iter()
        .filter(|(_, v)| !(v.msg == Msg::Delete && v.ts < drop_below))
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
