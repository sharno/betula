//! The log past the tree: batches a manifest doesn't hold yet, and reading
//! the log forward to find them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;

use crate::memtable::Memtable;
use crate::types::{Epoch, Version, WalSeq, entry_bytes};
use crate::wal::{WalBatch, WalRecord};

/// Consecutive log slots from some slot up to the first empty one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogRead {
    pub batches: Vec<(WalSeq, Arc<WalBatch>)>,
    pub fences: Vec<(WalSeq, Epoch)>,
    /// The first empty slot.
    pub end: WalSeq,
}

/// Reading the log forward one slot at a time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scan(LogRead);

impl Scan {
    pub fn from(seq: WalSeq) -> Self {
        Self(LogRead {
            batches: Vec::new(),
            fences: Vec::new(),
            end: seq,
        })
    }

    /// Carries on after `read`, from its first empty slot.
    pub fn resume(read: LogRead) -> Self {
        Self(read)
    }

    /// The slot to read next.
    pub fn next(&self) -> WalSeq {
        self.0.end
    }

    /// Feeds the record at `next()`: `Ok` once the slot was empty, `Err`
    /// to carry on.
    pub fn record(self, record: Option<WalRecord>) -> Result<LogRead, Self> {
        match record {
            Some(r) => Err(self.found(r)),
            None => Ok(self.0),
        }
    }

    /// The slot at `next()` holds `record`.
    pub fn found(mut self, record: WalRecord) -> Self {
        let at = self.0.end;
        match record {
            WalRecord::Batch(b) => self.0.batches.push((at, Arc::new(b))),
            WalRecord::Fence(e) => self.0.fences.push((at, e)),
        }
        self.0.end = at.next();
        self
    }
}

fn batch_bytes(b: &WalBatch) -> usize {
    b.ops
        .iter()
        .map(|(k, v)| entry_bytes(k, v.msg.encoded_len()))
        .sum()
}

/// Log batches past a manifest, merged newest-wins for reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tail {
    batches: BTreeMap<WalSeq, Arc<WalBatch>>,
    /// Fences past the manifest. A checkpoint that applies past one must
    /// record it (`Manifest::fences`).
    fences: BTreeSet<WalSeq>,
    mem: Memtable,
    /// Bytes of every batch held, which is what memory holds: `mem` keeps one
    /// version per key, so repeated overwrites barely grow it.
    retained: usize,
    /// The last slot known: applied, held, or a fence.
    last: WalSeq,
    /// When the oldest batch held arrived.
    pub since_ms: Option<u64>,
}

impl Tail {
    /// Nothing past `applied` yet.
    pub fn new(applied: WalSeq) -> Self {
        Self {
            batches: BTreeMap::new(),
            fences: BTreeSet::new(),
            mem: Memtable::default(),
            retained: 0,
            last: applied,
            since_ms: None,
        }
    }

    pub fn mem(&self) -> &Memtable {
        &self.mem
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained
    }

    /// The first slot past everything known.
    pub fn next(&self) -> WalSeq {
        self.last.next()
    }

    /// Whether more may be written under a backlog `limit`.
    pub fn has_room(&self, limit: usize) -> bool {
        self.retained < limit
    }

    pub fn add(&mut self, seq: WalSeq, batch: Arc<WalBatch>, now_ms: u64) {
        if seq <= self.last {
            return;
        }
        self.mem.apply(batch.ops.iter().cloned());
        self.retained += batch_bytes(&batch);
        self.batches.insert(seq, batch);
        self.last = seq;
        self.since_ms.get_or_insert(now_ms);
    }

    pub fn add_fence(&mut self, seq: WalSeq) {
        if seq > self.last {
            self.fences.insert(seq);
            self.last = seq;
        }
    }

    /// Adds what a forward read found past what's known.
    pub fn absorb(&mut self, read: LogRead, now_ms: u64) {
        let mut slots: Vec<(WalSeq, Option<Arc<WalBatch>>)> = read
            .batches
            .into_iter()
            .map(|(s, b)| (s, Some(b)))
            .chain(read.fences.into_iter().map(|(s, _)| (s, None)))
            .collect();
        slots.sort_by_key(|(s, _)| *s);
        slots.into_iter().for_each(|(seq, b)| match b {
            Some(b) => self.add(seq, b, now_ms),
            None => self.add_fence(seq),
        });
    }

    /// Drops what a manifest applying up to `applied` holds.
    pub fn prune(&mut self, applied: WalSeq, now_ms: u64) {
        let before = self.batches.len();
        self.batches = self.batches.split_off(&applied.next());
        self.fences = self.fences.split_off(&applied.next());
        self.last = self.last.max(applied);
        if self.batches.len() == before {
            return;
        }
        self.retained = self.batches.values().map(|b| batch_bytes(b)).sum();
        self.mem = self
            .batches
            .values()
            .fold(Memtable::default(), |mut mem, b| {
                mem.apply(b.ops.iter().cloned());
                mem
            });
        self.since_ms = (!self.is_empty()).then_some(now_ms);
    }

    /// What one checkpoint applies: the oldest batches, up to `max_bytes`
    /// (at least one, so a huge batch still goes). `None` with nothing held.
    pub fn take(&self, max_bytes: usize) -> Option<Taken> {
        let n = self
            .batches
            .values()
            .scan(0, |before, b| {
                let at = *before;
                *before += batch_bytes(b);
                Some(at)
            })
            .take_while(|at| *at == 0 || *at < max_bytes)
            .count();
        let taken: Vec<(&WalSeq, &Arc<WalBatch>)> = self.batches.iter().take(n).collect();
        let upto = *taken.last()?.0;
        let mem = taken.iter().fold(Memtable::default(), |mut mem, (_, b)| {
            mem.apply(b.ops.iter().cloned());
            mem
        });
        Some(Taken {
            msgs: mem.to_sorted_vec(),
            bytes: taken.iter().map(|(_, b)| batch_bytes(b)).sum(),
            upto,
            fences: self.fences.range(..=upto).copied().collect(),
            left: n < self.batches.len(),
        })
    }
}

/// One checkpoint's share of the log (`Tail::take`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Taken {
    pub msgs: Vec<(Bytes, Version)>,
    /// Bytes of the batches taken.
    pub bytes: usize,
    /// The last slot taken.
    pub upto: WalSeq,
    /// Fences at or below `upto`.
    pub fences: BTreeSet<WalSeq>,
    /// Batches were left for a later checkpoint.
    pub left: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Ts;
    use crate::types::Msg;

    fn batch(ts: u64, value: &'static [u8]) -> WalRecord {
        WalRecord::Batch(WalBatch {
            epoch: Epoch(1),
            ops: vec![(
                Bytes::from_static(b"k"),
                Version {
                    ts: Ts(ts),
                    msg: Msg::Put(Bytes::from_static(value)),
                },
            )],
        })
    }

    fn read(from: u64, records: Vec<WalRecord>) -> LogRead {
        let scan = records.into_iter().fold(Scan::from(WalSeq(from)), |s, r| {
            s.record(Some(r)).unwrap_err()
        });
        scan.record(None).unwrap()
    }

    #[test]
    fn scans_to_the_first_empty_slot_past_fences() {
        let r = read(
            3,
            vec![batch(1, b"a"), WalRecord::Fence(Epoch(2)), batch(2, b"b")],
        );
        assert_eq!(r.end, WalSeq(6));
        assert_eq!(r.fences, [(WalSeq(4), Epoch(2))]);
        assert_eq!(
            r.batches.iter().map(|(s, _)| s.0).collect::<Vec<_>>(),
            [3, 5]
        );
    }

    #[test]
    fn takes_a_prefix_with_its_fences_and_prunes_what_was_applied() {
        let mut tail = Tail::new(WalSeq(2));
        tail.absorb(
            read(
                3,
                vec![batch(1, b"a"), WalRecord::Fence(Epoch(2)), batch(2, b"bb")],
            ),
            10,
        );
        assert_eq!(tail.next(), WalSeq(6));
        assert_eq!(tail.mem().get(b"k").unwrap().ts, Ts(2));

        let first = tail.take(1).unwrap();
        assert_eq!((first.upto, first.left), (WalSeq(3), true));
        assert!(first.fences.is_empty());
        let all = tail.take(usize::MAX).unwrap();
        assert_eq!((all.upto, all.left), (WalSeq(5), false));
        assert_eq!(all.fences, [WalSeq(4)].into());

        tail.prune(WalSeq(3), 20);
        assert_eq!(tail.take(usize::MAX).unwrap().fences, [WalSeq(4)].into());
        tail.prune(WalSeq(5), 30);
        assert!(tail.is_empty() && tail.take(usize::MAX).is_none());
        assert_eq!((tail.next(), tail.since_ms), (WalSeq(6), None));
    }
}
