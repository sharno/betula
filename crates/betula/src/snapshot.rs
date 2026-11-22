use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;
use crate::memtable::{MemCursor, Memtable};
use crate::nodes::Nodes;
use crate::tree::{self, TreeIter};
use crate::types::{KeyRange, TreeRoot, Version};

/// A consistent read view: unapplied writer log tails over a tree version.
#[derive(Clone)]
pub struct Snapshot {
    pub tails: Memtable,
    pub tree: Option<TreeRoot>,
}

impl Snapshot {
    pub async fn get(&self, nodes: &Nodes, key: &[u8]) -> Result<Option<Version>> {
        tree::get(nodes, self.tree, key, self.tails.get(key).cloned()).await
    }

    pub fn scan(self, nodes: Arc<Nodes>, range: KeyRange) -> DbIterator {
        DbIterator {
            tails: MemCursor::new(self.tails, &range),
            tree: TreeIter::new(nodes, self.tree, range),
            tree_buf: VecDeque::new(),
            tree_done: false,
        }
    }
}

/// Ordered iterator over live key/value pairs in a range.
pub struct DbIterator {
    tails: MemCursor,
    tree: TreeIter,
    tree_buf: VecDeque<(Bytes, Version)>,
    tree_done: bool,
}

impl DbIterator {
    /// The next live pair and the timestamp of the version it came from.
    pub async fn next_versioned(&mut self) -> Result<Option<(Bytes, Version)>> {
        loop {
            if self.tree_buf.is_empty() && !self.tree_done {
                match self.tree.next_chunk().await? {
                    Some(chunk) => self.tree_buf = chunk.into(),
                    None => self.tree_done = true,
                }
            }
            let order = match (self.tails.peek(), self.tree_buf.front()) {
                (None, None) => return Ok(None),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some((a, _)), Some((b, _))) => a.cmp(b),
            };
            let (key, version) = match order {
                Ordering::Less => self.tails.pop(),
                Ordering::Greater => self.tree_buf.pop_front(),
                Ordering::Equal => {
                    let (k, a) = self.tails.pop().expect("peeked");
                    let (_, b) = self.tree_buf.pop_front().expect("peeked");
                    Some((k, if b.ts > a.ts { b } else { a }))
                }
            }
            .expect("one side was non-empty");
            if version.value().is_some() {
                return Ok(Some((key, version)));
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self.next_versioned().await?.and_then(|(k, v)| match v.msg {
            crate::types::Msg::Put(value) => Some((k, value)),
            crate::types::Msg::Delete => None,
        }))
    }

    /// The remaining pairs as a `Stream` (boxed, so it is `Unpin` and works
    /// with `StreamExt`/`TryStreamExt` directly).
    pub fn into_stream(self) -> futures::stream::BoxStream<'static, Result<(Bytes, Bytes)>> {
        use futures::StreamExt;
        futures::stream::try_unfold(self, |mut it| async move {
            Ok(it.next().await?.map(|kv| (kv, it)))
        })
        .boxed()
    }

    pub async fn collect(mut self) -> Result<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        while let Some(kv) = self.next().await? {
            out.push(kv);
        }
        Ok(out)
    }
}
