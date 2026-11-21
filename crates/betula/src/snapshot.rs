use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;
use crate::memtable::{MemMerge, Memtable};
use crate::nodes::Nodes;
use crate::tree::{self, TreeIter};
use crate::types::{KeyRange, Msg, TreeRoot};

/// A consistent read view: memtables (newest first) over a tree version.
#[derive(Clone)]
pub struct Snapshot {
    pub mems: Vec<Memtable>,
    pub tree: Option<TreeRoot>,
}

impl Snapshot {
    pub async fn get(&self, nodes: &Nodes, key: &[u8]) -> Result<Option<Bytes>> {
        match self.mems.iter().find_map(|m| m.get(key)) {
            Some(msg) => Ok(msg.clone().into_value()),
            None => tree::get(nodes, self.tree, key).await,
        }
    }

    pub fn scan(self, nodes: Arc<Nodes>, range: KeyRange) -> DbIterator {
        DbIterator {
            mem: MemMerge::new(self.mems, &range),
            tree: TreeIter::new(nodes, self.tree, range),
            tree_buf: VecDeque::new(),
            tree_done: false,
        }
    }
}

/// Ordered iterator over live key/value pairs in a range.
pub struct DbIterator {
    mem: MemMerge,
    tree: TreeIter,
    tree_buf: VecDeque<(Bytes, Bytes)>,
    tree_done: bool,
}

enum Pick {
    Mem,
    Tree,
    /// Same key on both sides: the memtable is newer.
    MemShadowingTree,
}

impl DbIterator {
    pub async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        loop {
            if self.tree_buf.is_empty() && !self.tree_done {
                match self.tree.next_chunk().await? {
                    Some(chunk) => self.tree_buf = chunk.into(),
                    None => self.tree_done = true,
                }
            }
            let pick = match (self.mem.peek_key(), self.tree_buf.front()) {
                (None, None) => return Ok(None),
                (Some(_), None) => Pick::Mem,
                (None, Some(_)) => Pick::Tree,
                (Some(mk), Some((tk, _))) => match mk.cmp(tk) {
                    Ordering::Less => Pick::Mem,
                    Ordering::Greater => Pick::Tree,
                    Ordering::Equal => Pick::MemShadowingTree,
                },
            };
            let mem_msg = match pick {
                Pick::Tree => return Ok(self.tree_buf.pop_front()),
                Pick::Mem => self.mem.pop(),
                Pick::MemShadowingTree => {
                    self.tree_buf.pop_front();
                    self.mem.pop()
                }
            };
            if let Some((k, Msg::Put(v))) = mem_msg {
                return Ok(Some((k, v)));
            }
        }
    }

    pub async fn collect(mut self) -> Result<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        while let Some(kv) = self.next().await? {
            out.push(kv);
        }
        Ok(out)
    }
}
