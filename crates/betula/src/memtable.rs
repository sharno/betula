use std::collections::VecDeque;

use bytes::Bytes;
use imbl::OrdMap;

use crate::types::{KeyRange, Msg, entry_bytes};

/// Persistent sorted map: clones are O(1), so read snapshots don't copy.
#[derive(Clone, Debug, Default)]
pub struct Memtable {
    map: OrdMap<Bytes, Msg>,
    bytes: usize,
}

impl Memtable {
    pub fn apply(&mut self, ops: impl IntoIterator<Item = (Bytes, Msg)>) {
        ops.into_iter().for_each(|(k, m)| {
            let added = entry_bytes(&k, m.encoded_len());
            let removed = self
                .map
                .insert(k.clone(), m)
                .map_or(0, |old| entry_bytes(&k, old.encoded_len()));
            self.bytes = self.bytes + added - removed;
        });
    }

    pub fn get(&self, key: &[u8]) -> Option<&Msg> {
        self.map.get(key)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn to_sorted_vec(&self) -> Vec<(Bytes, Msg)> {
        self.map
            .iter()
            .map(|(k, m)| (k.clone(), m.clone()))
            .collect()
    }

    fn page(&self, range: &KeyRange, limit: usize) -> Vec<(Bytes, Msg)> {
        self.map
            .range((range.start.clone(), range.end.clone()))
            .take(limit)
            .map(|(k, m)| (k.clone(), m.clone()))
            .collect()
    }
}

const PAGE: usize = 256;

/// Lazily pages through one memtable's range without borrowing it.
struct MemCursor {
    mem: Memtable,
    range: KeyRange,
    buf: VecDeque<(Bytes, Msg)>,
    exhausted: bool,
}

impl MemCursor {
    fn peek(&mut self) -> Option<&(Bytes, Msg)> {
        if self.buf.is_empty() && !self.exhausted {
            let page = self.mem.page(&self.range, PAGE);
            self.exhausted = page.len() < PAGE;
            if let Some((last, _)) = page.last() {
                self.range = self.range.resume_after(last.clone());
            }
            self.buf = page.into();
        }
        self.buf.front()
    }
}

/// Merges several memtables, newest first; the newest message per key wins.
pub struct MemMerge {
    cursors: Vec<MemCursor>,
}

impl MemMerge {
    pub fn new(mems: Vec<Memtable>, range: &KeyRange) -> Self {
        Self {
            cursors: mems
                .into_iter()
                .map(|mem| MemCursor {
                    mem,
                    range: range.clone(),
                    buf: VecDeque::new(),
                    exhausted: false,
                })
                .collect(),
        }
    }

    pub fn peek_key(&mut self) -> Option<Bytes> {
        self.cursors
            .iter_mut()
            .filter_map(|c| c.peek().map(|(k, _)| k.clone()))
            .min()
    }

    /// Pops the smallest key, taking the newest message and dropping older
    /// ones for the same key.
    pub fn pop(&mut self) -> Option<(Bytes, Msg)> {
        let key = self.peek_key()?;
        self.cursors.iter_mut().fold(None, |winner, c| {
            let hit = c.peek().is_some_and(|(k, _)| *k == key);
            let popped = hit.then(|| c.buf.pop_front()).flatten();
            winner.or(popped)
        })
    }
}
