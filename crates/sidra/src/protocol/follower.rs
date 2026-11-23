//! What a follower knows (the newest manifest, every writer's entries not
//! yet in it, which writers to poll) and how log reads and seals change it.
//! The driver (`follow::Follower`) issues the reads these decide on.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::hlc::WriterId;
use crate::manifest::{Frontier, Manifest};
use crate::memtable::Memtable;
use crate::types::{ManifestId, entry_bytes};
use crate::wal::{WalBatch, WalRecord};

/// Writer log entries not yet in the tree, merged newest-wins.
#[derive(Clone, Debug, Default)]
pub struct Tails {
    batches: BTreeMap<WriterId, BTreeMap<u64, Arc<WalBatch>>>,
    mem: Memtable,
    /// Bytes of every batch held, which is what memory holds: `mem` keeps one
    /// version per key, so repeated overwrites barely grow it.
    retained: usize,
    /// When the oldest unapplied entry was first seen.
    pub since_ms: Option<u64>,
    /// Seals no manifest records yet, by sequence number. Every entry of the
    /// writer between the manifest's frontier and its seal is held here.
    sealed: BTreeMap<WriterId, u64>,
}

fn batch_bytes(b: &WalBatch) -> usize {
    b.ops
        .iter()
        .map(|(k, v)| entry_bytes(k, v.msg.encoded_len()))
        .sum()
}

impl Tails {
    pub fn mem(&self) -> &Memtable {
        &self.mem
    }

    pub fn is_empty(&self) -> bool {
        self.batches.values().all(BTreeMap::is_empty)
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained
    }

    /// Writers whose seal was found since the last manifest.
    pub fn sealed(&self) -> impl Iterator<Item = WriterId> + '_ {
        self.sealed.keys().copied()
    }

    pub fn add(&mut self, seq: u64, batch: Arc<WalBatch>, now_ms: u64) {
        let log = self.batches.entry(batch.writer).or_default();
        if log.contains_key(&seq) {
            return;
        }
        self.mem.apply(batch.ops.iter().cloned());
        self.retained += batch_bytes(&batch);
        log.insert(seq, batch);
        self.since_ms.get_or_insert(now_ms);
    }

    /// Drops entries (and seals) the manifest holds.
    pub fn prune(&mut self, m: &Manifest, now_ms: u64) {
        self.batches.iter_mut().for_each(|(w, log)| {
            let upto = m.frontier.get(w).copied().unwrap_or(0);
            log.retain(|seq, _| *seq > upto);
        });
        self.batches.retain(|_, log| !log.is_empty());
        self.sealed.retain(|w, _| !m.sealed.contains(w));
        self.retained = self
            .batches
            .values()
            .flat_map(|log| log.values())
            .map(|b| batch_bytes(b))
            .sum();
        self.mem = self.batches.values().flat_map(|log| log.values()).fold(
            Memtable::default(),
            |mut mem, b| {
                mem.apply(b.ops.iter().cloned());
                mem
            },
        );
        self.since_ms = (!self.is_empty()).then_some(now_ms);
    }

    /// The first sequence number of `writer` neither held nor applied.
    pub fn next_seq(&self, writer: WriterId, applied: &Frontier) -> u64 {
        let known = self
            .batches
            .get(&writer)
            .and_then(|log| log.last_key_value())
            .map_or(0, |(seq, _)| *seq);
        known.max(applied.get(&writer).copied().unwrap_or(0)) + 1
    }

    /// `applied` advanced past every entry held here.
    pub fn frontier(&self, applied: &Frontier) -> Frontier {
        let mut f = applied.clone();
        self.batches.iter().for_each(|(w, log)| {
            if let Some((seq, _)) = log.last_key_value() {
                f.insert(*w, *seq);
            }
        });
        f
    }
}

/// What a refresh pulls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// New manifests and every other writer's unapplied log entries.
    AllWriters,
    /// New manifests only: other writers' writes appear once applied.
    ManifestOnly,
}

/// Writers whose logs a refresh pulls.
///
/// Every writer that can still write has a lease (written before its first
/// log entry), and GC deletes a lease only once the writer is sealed and its
/// log applied. So a writer missing from a `writers/` LIST has nothing left
/// to pull, even while the manifest frontier still names it, and polling it
/// would only fail once its log store goes down.
///
/// Frontier writers are pulled too (a new manifest may name a writer newer
/// than the last LIST), except those the last LIST showed gone.
#[derive(Debug, Default)]
pub struct Tracked {
    /// Writers seen in storage (by a LIST or `track`).
    present: BTreeSet<WriterId>,
    /// Frontier writers the last LIST didn't find.
    gone: BTreeSet<WriterId>,
}

impl Tracked {
    pub fn track(&mut self, writers: impl IntoIterator<Item = WriterId>) {
        self.present.extend(writers);
    }

    /// Writers to pull, given the frontier being applied against.
    pub fn pulled(&self, applied: &Frontier) -> BTreeSet<WriterId> {
        applied
            .keys()
            .filter(|w| !self.gone.contains(w))
            .chain(&self.present)
            .copied()
            .collect()
    }

    /// Applies a LIST that started when `before` was pulled under `frontier`,
    /// returning the writers it found gone. Only writers pulled before the
    /// LIST can be gone: one tracked meanwhile (from a newer LIST) may be too
    /// new for it.
    pub fn listed(
        &mut self,
        before: &BTreeSet<WriterId>,
        frontier: &Frontier,
        found: BTreeSet<WriterId>,
    ) -> BTreeSet<WriterId> {
        let forgotten: BTreeSet<WriterId> = before.difference(&found).copied().collect();
        self.gone = frontier
            .keys()
            .filter(|w| !found.contains(w))
            .copied()
            .collect();
        self.present = self
            .present
            .difference(&forgotten)
            .copied()
            .chain(found)
            .collect();
        forgotten
    }
}

/// Where a read of a log stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogEnd {
    /// Nothing at `next` yet.
    Open { next: u64 },
    /// The log is sealed at `at`.
    Sealed { at: u64 },
}

/// Consecutive entries of one log and where they stopped.
#[derive(Debug)]
pub struct LogRead {
    pub entries: Vec<(u64, Arc<WalBatch>)>,
    pub end: LogEnd,
}

/// Reading a log forward from a sequence number, one record at a time.
#[derive(Debug)]
pub struct Reading {
    entries: Vec<(u64, Arc<WalBatch>)>,
    next: u64,
}

impl Reading {
    pub fn from(seq: u64) -> Self {
        Self {
            entries: Vec::new(),
            next: seq,
        }
    }

    /// The sequence number to read next.
    pub fn next(&self) -> u64 {
        self.next
    }

    /// Feeds the record at `next()`; `Err` carries on reading.
    pub fn record(mut self, record: Option<WalRecord>) -> Result<LogRead, Self> {
        match record {
            Some(WalRecord::Batch(b)) => {
                self.entries.push((self.next, Arc::new(b)));
                self.next += 1;
                Err(self)
            }
            Some(WalRecord::Seal) => Ok(LogRead {
                entries: self.entries,
                end: LogEnd::Sealed { at: self.next },
            }),
            None => Ok(LogRead {
                entries: self.entries,
                end: LogEnd::Open { next: self.next },
            }),
        }
    }
}

/// Sealing a log: read to its end, put a seal in the next slot, and if a
/// batch got there first, take it and try the slot after.
#[derive(Debug)]
pub enum Sealing {
    /// Reading records; feed them with `record`.
    Reading(Reading),
    /// Put-if-absent a seal at `at`; feed the outcome with `sealed`/`lost`.
    Placing {
        entries: Vec<(u64, Arc<WalBatch>)>,
        at: u64,
    },
    /// The log is sealed; every entry before the seal is in `read`.
    Done(LogRead),
}

impl Sealing {
    pub fn from(seq: u64) -> Self {
        Self::Reading(Reading::from(seq))
    }

    /// Only valid while `Reading`.
    pub fn record(self, record: Option<WalRecord>) -> Self {
        match self {
            Self::Reading(r) => match r.record(record) {
                Err(more) => Self::Reading(more),
                Ok(LogRead {
                    entries,
                    end: LogEnd::Open { next },
                }) => Self::Placing { entries, at: next },
                Ok(read) => Self::Done(read),
            },
            other => other,
        }
    }

    /// The seal went in at `at`. Only valid while `Placing`.
    pub fn sealed(self) -> Self {
        match self {
            Self::Placing { entries, at } => Self::Done(LogRead {
                entries,
                end: LogEnd::Sealed { at },
            }),
            other => other,
        }
    }

    /// A batch held the slot: keep reading past it. Only valid while
    /// `Placing`.
    pub fn lost(self, batch: WalBatch) -> Self {
        match self {
            Self::Placing { mut entries, at } => {
                entries.push((at, Arc::new(batch)));
                Self::Reading(Reading {
                    entries,
                    next: at + 1,
                })
            }
            other => other,
        }
    }
}

/// A follower's view: the newest manifest it has and the entries beyond it.
#[derive(Clone, Debug)]
pub struct View {
    pub manifest_id: ManifestId,
    pub manifest: Arc<Manifest>,
    pub tails: Tails,
}

impl View {
    /// Whether `writer`'s log is sealed with every entry before the seal
    /// either applied or held in `tails`.
    pub fn is_sealed(&self, writer: WriterId) -> bool {
        self.manifest.sealed.contains(&writer) || self.tails.sealed.contains_key(&writer)
    }

    /// Adds what was read from `writer`'s log, skipping applied entries.
    pub fn absorb(&mut self, writer: WriterId, read: LogRead, now_ms: u64) {
        let upto = self.manifest.frontier.get(&writer).copied().unwrap_or(0);
        read.entries
            .into_iter()
            .filter(|(seq, _)| *seq > upto)
            .for_each(|(seq, batch)| self.tails.add(seq, batch, now_ms));
        match read.end {
            LogEnd::Sealed { at } if !self.manifest.sealed.contains(&writer) => {
                self.tails.sealed.insert(writer, at);
            }
            LogEnd::Sealed { .. } | LogEnd::Open { .. } => {}
        }
    }

    /// Moves to a newer manifest, dropping what it holds.
    pub fn install(&mut self, id: ManifestId, manifest: Arc<Manifest>, now_ms: u64) {
        if id > self.manifest_id {
            self.tails.prune(&manifest, now_ms);
            self.manifest_id = id;
            self.manifest = manifest;
        }
    }

    /// Adds this process's own durable batch.
    pub fn add_own(&mut self, seq: u64, batch: Arc<WalBatch>, now_ms: u64) {
        if seq
            > self
                .manifest
                .frontier
                .get(&batch.writer)
                .copied()
                .unwrap_or(0)
        {
            self.tails.add(seq, batch, now_ms);
        }
    }

    /// Where to read each polled writer's log from, against manifest `m`
    /// (the view's, or a newer one about to be installed). Sealed logs and
    /// this process's own aren't polled.
    pub fn reads(
        &self,
        m: &Manifest,
        tracked: &Tracked,
        own: Option<WriterId>,
        scope: Scope,
    ) -> Vec<(WriterId, u64)> {
        match scope {
            Scope::ManifestOnly => Vec::new(),
            Scope::AllWriters => tracked
                .pulled(&m.frontier)
                .into_iter()
                .filter(|w| Some(*w) != own)
                .filter(|w| !m.sealed.contains(w) && !self.tails.sealed.contains_key(w))
                .map(|w| (w, self.tails.next_seq(w, &m.frontier)))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(w: u64) -> WalBatch {
        WalBatch {
            writer: WriterId(w),
            ops: Vec::new(),
        }
    }

    #[test]
    fn sealing_reads_to_the_end_and_steps_past_a_racing_batch() {
        let s = Sealing::from(3)
            .record(Some(WalRecord::Batch(batch(1))))
            .record(None);
        assert!(matches!(s, Sealing::Placing { at: 4, .. }));
        let s = s.lost(batch(1)).record(None);
        assert!(matches!(s, Sealing::Placing { at: 5, .. }));
        let Sealing::Done(read) = s.sealed() else {
            panic!("sealed");
        };
        assert_eq!(read.end, LogEnd::Sealed { at: 5 });
        assert_eq!(
            read.entries.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            [3, 4]
        );
        let Sealing::Done(read) = Sealing::from(7).record(Some(WalRecord::Seal)) else {
            panic!("already sealed");
        };
        assert_eq!(read.end, LogEnd::Sealed { at: 7 });
    }

    #[test]
    fn sealed_and_own_logs_are_not_polled() {
        let mut tracked = Tracked::default();
        tracked.track([WriterId(1), WriterId(2), WriterId(3)]);
        let m = Manifest {
            sealed: [WriterId(2)].into(),
            frontier: [(WriterId(1), 4)].into(),
            ..Manifest::initial(0)
        };
        let view = View {
            manifest_id: ManifestId(1),
            manifest: Arc::new(m.clone()),
            tails: Tails::default(),
        };
        let reads = view.reads(&m, &tracked, Some(WriterId(3)), Scope::AllWriters);
        assert_eq!(reads, [(WriterId(1), 5)]);
        assert!(
            view.reads(&m, &tracked, None, Scope::ManifestOnly)
                .is_empty()
        );
    }
}
