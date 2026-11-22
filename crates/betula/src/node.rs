use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::{Msg, NodePtr, entry_bytes};

/// Tree node. Nodes are immutable once written: every change produces a new
/// node under a new id (copy-on-write).
/// Variants are `Arc`ed so cache hits and iterator frames are cheap clones.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Node {
    Leaf(Arc<Leaf>),
    Internal(Arc<Internal>),
}

/// Sorted, unique key/value pairs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Leaf {
    pub entries: Vec<(Bytes, Bytes)>,
}

/// `children[i]` covers keys in `[pivots[i-1], pivots[i])`, so
/// `children.len() == pivots.len() + 1`. `buffer` is sorted, holds at most one
/// message per key, and its messages are newer than anything below.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Internal {
    pub pivots: Vec<Bytes>,
    pub children: Vec<NodePtr>,
    pub buffer: Vec<(Bytes, Msg)>,
}

impl Leaf {
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        self.entries
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }
}

impl Internal {
    pub fn child_index(&self, key: &[u8]) -> usize {
        self.pivots.partition_point(|p| p.as_ref() <= key)
    }

    pub fn buffered(&self, key: &[u8]) -> Option<&Msg> {
        self.buffer
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|i| &self.buffer[i].1)
    }

    /// First child that can hold keys at or after `start`.
    pub fn first_child(&self, start: &Bound<Bytes>) -> usize {
        match start {
            Bound::Included(k) | Bound::Excluded(k) => self.child_index(k),
            Bound::Unbounded => 0,
        }
    }

    /// Last child that can hold keys at or before `end`.
    pub fn last_child(&self, end: &Bound<Bytes>) -> usize {
        match end {
            Bound::Included(k) => self.child_index(k),
            // Keys strictly below a pivot never land in the pivot's child.
            Bound::Excluded(k) => self.pivots.partition_point(|p| p.as_ref() < k.as_ref()),
            Bound::Unbounded => self.pivots.len(),
        }
    }

    /// Key bounds of child `i`.
    pub fn child_bounds(&self, i: usize) -> (Option<&Bytes>, Option<&Bytes>) {
        let lo = i.checked_sub(1).map(|j| &self.pivots[j]);
        (lo, self.pivots.get(i))
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
        self.pivots.iter().map(|p| p.len() + 8).sum::<usize>() + self.children.len() * 32
    }

    /// Buffered bytes per child, in child order.
    pub fn pending_per_child(&self) -> Vec<usize> {
        (0..self.children.len())
            .map(|i| msgs_bytes(&self.buffer[self.buffer_slice(i)]))
            .collect()
    }
}

pub fn msgs_bytes(msgs: &[(Bytes, Msg)]) -> usize {
    msgs.iter()
        .map(|(k, m)| entry_bytes(k, m.encoded_len()))
        .sum()
}

pub fn entries_bytes(entries: &[(Bytes, Bytes)]) -> usize {
    entries.iter().map(|(k, v)| entry_bytes(k, v.len())).sum()
}

/// Merges two sorted unique message runs; `newer` wins on equal keys.
pub fn merge_msgs(older: Vec<(Bytes, Msg)>, newer: Vec<(Bytes, Msg)>) -> Vec<(Bytes, Msg)> {
    merge_sorted(older, newer)
}

/// Applies sorted unique messages to sorted unique leaf entries.
pub fn apply_to_entries(
    entries: Vec<(Bytes, Bytes)>,
    msgs: Vec<(Bytes, Msg)>,
) -> Vec<(Bytes, Bytes)> {
    let entries = entries.into_iter().map(|(k, v)| (k, Msg::Put(v))).collect();
    merge_sorted(entries, msgs)
        .into_iter()
        .filter_map(|(k, m)| m.into_value().map(|v| (k, v)))
        .collect()
}

fn merge_sorted<V>(older: Vec<(Bytes, V)>, newer: Vec<(Bytes, V)>) -> Vec<(Bytes, V)> {
    let mut out = Vec::with_capacity(older.len() + newer.len());
    let mut a = older.into_iter().peekable();
    let mut b = newer.into_iter().peekable();
    loop {
        let take_newer = match (a.peek(), b.peek()) {
            (None, None) => break,
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (Some((ka, _)), Some((kb, _))) => {
                if ka == kb {
                    a.next();
                    true
                } else {
                    kb < ka
                }
            }
        };
        out.push(if take_newer { b.next() } else { a.next() }.expect("peeked"));
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
