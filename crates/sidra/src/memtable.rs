use std::collections::VecDeque;

use bytes::Bytes;
use imbl::OrdMap;

use crate::types::{KeyRange, Version, entry_bytes};

/// Persistent sorted map holding the newest version per key. Clones are O(1),
/// so read snapshots don't copy. Inserting is order-independent: a version
/// only replaces an older one.
#[derive(Clone, Debug, Default)]
pub struct Memtable {
    map: OrdMap<Bytes, Version>,
    bytes: usize,
}

impl Memtable {
    pub fn apply(&mut self, ops: impl IntoIterator<Item = (Bytes, Version)>) {
        ops.into_iter().for_each(|(k, v)| {
            let replaced = match self.map.get(&k) {
                Some(old) if old.ts >= v.ts => return,
                Some(old) => entry_bytes(&k, old.msg.encoded_len()),
                None => 0,
            };
            self.bytes = self.bytes + entry_bytes(&k, v.msg.encoded_len()) - replaced;
            self.map.insert(k, v);
        });
    }

    pub fn get(&self, key: &[u8]) -> Option<&Version> {
        self.map.get(key)
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn to_sorted_vec(&self) -> Vec<(Bytes, Version)> {
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn page(&self, range: &KeyRange, limit: usize) -> Vec<(Bytes, Version)> {
        self.map
            .range((range.start.clone(), range.end.clone()))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

const PAGE: usize = 256;

/// Lazily pages through a memtable's range without borrowing it.
pub struct MemCursor {
    mem: Memtable,
    range: KeyRange,
    buf: VecDeque<(Bytes, Version)>,
    exhausted: bool,
}

impl MemCursor {
    pub fn new(mem: Memtable, range: &KeyRange) -> Self {
        Self {
            mem,
            range: range.clone(),
            buf: VecDeque::new(),
            exhausted: false,
        }
    }

    pub fn peek(&mut self) -> Option<&(Bytes, Version)> {
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

    pub fn pop(&mut self) -> Option<(Bytes, Version)> {
        self.peek();
        self.buf.pop_front()
    }
}
