use std::fmt;
use std::ops::Bound;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::hlc::Ts;

/// Writer generation. Bumped by every writer that opens the database; a writer
/// whose epoch is no longer the latest is fenced.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A checkpoint's output object. The epoch prefix makes ids unique across
/// writers without coordination, and lets a new writer find a crashed
/// writer's orphans with one ordered listing.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId {
    pub epoch: Epoch,
    pub seq: u64,
}

impl SegmentId {
    pub fn next(self) -> Self {
        Self {
            seq: self.seq + 1,
            ..self
        }
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.epoch.0, self.seq)
    }
}

/// Where an immutable node lives: a byte range of a segment. Parents store
/// these directly, so reading a node is one ranged GET with no lookup.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodePtr {
    pub seg: SegmentId,
    pub offset: u64,
    pub len: u32,
}

impl NodePtr {
    pub fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + u64::from(self.len)
    }
}

impl fmt::Display for NodePtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}+{}", self.seg, self.offset, self.len)
    }
}

/// Root of a non-empty tree. `height` 0 means the root is a leaf.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeRoot {
    pub ptr: NodePtr,
    /// Oldest tombstone in the tree's leaves.
    pub oldest_tombstone: Option<Ts>,
    pub height: u32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ManifestId(pub u64);

impl ManifestId {
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A mutation for a key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Msg {
    Put(Bytes),
    Delete,
}

impl Msg {
    pub fn into_value(self) -> Option<Bytes> {
        match self {
            Msg::Put(v) => Some(v),
            Msg::Delete => None,
        }
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            Msg::Put(v) => v.len(),
            Msg::Delete => 0,
        }
    }
}

/// A timestamped mutation. Wherever two versions of a key meet, the larger
/// timestamp wins, so applying writes in any order gives the same result.
/// Buffers and leaves hold at most one version per key; deletes are kept as
/// tombstones until no older write can still arrive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub ts: Ts,
    pub msg: Msg,
}

impl Version {
    pub fn value(&self) -> Option<&Bytes> {
        match &self.msg {
            Msg::Put(v) => Some(v),
            Msg::Delete => None,
        }
    }

    /// The newer of two versions.
    pub fn newest(a: Option<Version>, b: Option<Version>) -> Option<Version> {
        match (a, b) {
            (Some(a), Some(b)) => Some(if b.ts > a.ts { b } else { a }),
            (a, b) => a.or(b),
        }
    }
}

/// Approximate in-memory/on-disk cost of an entry, used for node sizing.
pub fn entry_bytes(key: &Bytes, msg_len: usize) -> usize {
    key.len() + msg_len + 24
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
}

impl KeyRange {
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    pub fn new(start: Bound<Bytes>, end: Bound<Bytes>) -> Self {
        Self { start, end }
    }

    pub fn after_start(&self, key: &[u8]) -> bool {
        match &self.start {
            Bound::Included(s) => key >= s.as_ref(),
            Bound::Excluded(s) => key > s.as_ref(),
            Bound::Unbounded => true,
        }
    }

    pub fn before_end(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Included(e) => key <= e.as_ref(),
            Bound::Excluded(e) => key < e.as_ref(),
            Bound::Unbounded => true,
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.after_start(key) && self.before_end(key)
    }

    /// The same range with its start moved past `key`.
    pub fn resume_after(&self, key: Bytes) -> Self {
        Self {
            start: Bound::Excluded(key),
            end: self.end.clone(),
        }
    }
}

impl<R: std::ops::RangeBounds<Bytes>> From<R> for KeyRange {
    fn from(r: R) -> Self {
        Self {
            start: r.start_bound().cloned(),
            end: r.end_bound().cloned(),
        }
    }
}
