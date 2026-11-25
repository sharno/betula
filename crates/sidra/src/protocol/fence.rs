//! Opening a writer: after claiming the next epoch in the manifest, read the
//! log forward from the manifest's applied slot and put a fence in the first
//! empty slot. Every batch an older writer made durable is then before the
//! fence, and its next put lands on the fence and fails.
//!
//! ```text
//! Reading --slot empty--> Placing --placed--> Fenced
//!    ^                       |
//!    +----slot taken---------+
//! ```
//!
//! An older writer may win the slot first (its batch, or an older opener's
//! fence): the new writer reads it and tries the next slot. A newer writer's
//! fence means this one was superseded while opening.

use crate::protocol::tail::{LogRead, Scan};
use crate::types::{Epoch, WalSeq};
use crate::wal::{FenceAt, WalRecord};

/// A newer writer fenced the log while this one was opening.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Superseded(pub Epoch);

/// Reading the log forward; feed the record at `next()`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Reading {
    epoch: Epoch,
    scan: Scan,
}

/// Put-if-absent this writer's fence at `at()`; feed the outcome.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Placing {
    epoch: Epoch,
    read: LogRead,
}

/// The fence is in place: `read` holds every slot before it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Fenced {
    pub read: LogRead,
    pub fence: WalSeq,
}

pub enum AfterRead {
    More(Reading),
    Place(Placing),
}

pub enum AfterPlace {
    Done(Fenced),
    /// Something took the slot first: read on from it.
    More(Reading),
}

fn check(epoch: Epoch, record: Option<&WalRecord>) -> Result<(), Superseded> {
    match record {
        Some(WalRecord::Fence(e)) if *e > epoch => Err(Superseded(*e)),
        Some(_) | None => Ok(()),
    }
}

impl Reading {
    /// Starts after the manifest's applied slot.
    pub fn new(epoch: Epoch, applied: WalSeq) -> Self {
        Self {
            epoch,
            scan: Scan::from(applied.next()),
        }
    }

    pub fn next(&self) -> WalSeq {
        self.scan.next()
    }

    pub fn record(self, record: Option<WalRecord>) -> Result<AfterRead, Superseded> {
        check(self.epoch, record.as_ref())?;
        Ok(match self.scan.record(record) {
            Err(scan) => AfterRead::More(Reading { scan, ..self }),
            Ok(read) => AfterRead::Place(Placing {
                epoch: self.epoch,
                read,
            }),
        })
    }
}

impl Placing {
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn at(&self) -> WalSeq {
        self.read.end
    }

    pub fn outcome(self, outcome: FenceAt) -> Result<AfterPlace, Superseded> {
        match outcome {
            FenceAt::Placed => {
                let fence = self.read.end;
                Ok(AfterPlace::Done(Fenced {
                    read: self.read,
                    fence,
                }))
            }
            FenceAt::Taken(record) => {
                check(self.epoch, Some(&record))?;
                let scan = Scan::resume(self.read).found(record);
                Ok(AfterPlace::More(Reading {
                    epoch: self.epoch,
                    scan,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalBatch;

    fn batch() -> WalRecord {
        WalRecord::Batch(WalBatch {
            epoch: Epoch(1),
            ops: Vec::new(),
        })
    }

    fn placing(r: Reading, record: Option<WalRecord>) -> Placing {
        match r.record(record).unwrap() {
            AfterRead::Place(p) => p,
            AfterRead::More(_) => panic!("expected to place"),
        }
    }

    #[test]
    fn fences_the_first_empty_slot_and_steps_past_a_racing_batch() {
        let AfterRead::More(r) = Reading::new(Epoch(2), WalSeq(4))
            .record(Some(batch()))
            .unwrap()
        else {
            panic!("reads on");
        };
        let p = placing(r, None);
        assert_eq!(p.at(), WalSeq(6));
        let AfterPlace::More(r) = p.outcome(FenceAt::Taken(batch())).unwrap() else {
            panic!("reads on past the racing batch");
        };
        let p = placing(r, None);
        assert_eq!(p.at(), WalSeq(7));
        let AfterPlace::Done(f) = p.outcome(FenceAt::Placed).unwrap() else {
            panic!("placed");
        };
        assert_eq!(f.fence, WalSeq(7));
        assert_eq!(
            f.read.batches.iter().map(|(s, _)| s.0).collect::<Vec<_>>(),
            [5, 6]
        );
    }

    #[test]
    fn reads_past_older_fences_and_stops_at_a_newer_one() {
        let r = Reading::new(Epoch(5), WalSeq(0));
        let AfterRead::More(r) = r.record(Some(WalRecord::Fence(Epoch(4)))).unwrap() else {
            panic!("an older fence is read past");
        };
        assert_eq!(
            r.record(Some(WalRecord::Fence(Epoch(6)))).err(),
            Some(Superseded(Epoch(6)))
        );
        let p = placing(Reading::new(Epoch(5), WalSeq(0)), None);
        assert_eq!(
            p.outcome(FenceAt::Taken(WalRecord::Fence(Epoch(7)))).err(),
            Some(Superseded(Epoch(7)))
        );
    }
}
