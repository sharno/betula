//! A writer's log as a state machine: batching writes, writing each batch to
//! the next sequence number with put-if-absent, acknowledging once durable,
//! and stopping for good when a put lands on a seal, finds the id taken, or
//! fails with an unknown outcome.
//!
//! At most one batch is being written at a time; writes arriving meanwhile
//! join the next batch. The driver runs `Put` effects and feeds the outcome
//! back as `PutDone`.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::hlc::WriterId;
use crate::types::{Version, entry_bytes};
use crate::wal::WalBatch;

/// Timestamps handed out but not yet durable (a multiset). A writer's lease
/// floor must stay at or below the oldest, and the caller registers a
/// timestamp before handing its write to the log, so no lease can miss it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Inflight(BTreeMap<u64, usize>);

impl Inflight {
    pub fn register(&mut self, hlc: u64) {
        *self.0.entry(hlc).or_default() += 1;
    }

    pub fn release(&mut self, stamps: &[u64]) {
        stamps.iter().for_each(|t| {
            if let Some(n) = self.0.get_mut(t) {
                *n -= 1;
                if *n == 0 {
                    self.0.remove(t);
                }
            }
        });
    }

    /// The lease floor given a fresh clock reading `now_hlc`.
    pub fn floor(&self, now_hlc: u64) -> u64 {
        self.0
            .keys()
            .next()
            .map_or(now_hlc, |&oldest| oldest.min(now_hlc))
    }
}

/// How a put of a log batch ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PutOutcome {
    Written,
    /// A seal holds the slot: the writer was presumed dead.
    Sealed,
    /// Another process wrote a different batch there under this id.
    Taken,
    /// Failed after retries: whether it landed is unknown.
    Failed,
}

/// Why the log stopped. Terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stopped {
    Sealed,
    Taken,
    /// A put's outcome is unknown, so the next batch can't reuse its
    /// sequence number.
    Unknown,
}

/// Whether the driver lets a batch go out now: closed while too much is
/// unapplied (backpressure).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Gate {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Event<W> {
    Write {
        hlc: u64,
        ops: Vec<(Bytes, Version)>,
        waiter: Option<W>,
    },
    /// Acknowledge `waiter` once everything written before is durable.
    Flush {
        waiter: W,
    },
    /// The flush interval elapsed.
    Tick,
    /// No more writes: write out what's pending, ignoring backpressure.
    Close,
    PutDone {
        seq: u64,
        outcome: PutOutcome,
    },
    /// Backpressure may have eased.
    Retry,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect<W> {
    /// Put-if-absent this batch at `seq`.
    Put { seq: u64, batch: Arc<WalBatch> },
    /// The batch at `seq` is durable. `stamps` leave `Inflight`; `writes`
    /// is how many write calls it holds.
    Durable {
        seq: u64,
        batch: Arc<WalBatch>,
        stamps: Vec<u64>,
        writes: u64,
    },
    Ack {
        waiters: Vec<W>,
        result: Result<(), Stopped>,
    },
    /// The log stopped; nothing more can be written under this id.
    Stopped { why: Stopped, stamps: Vec<u64> },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Batch<W> {
    ops: Vec<(Bytes, Version)>,
    stamps: Vec<u64>,
    waiters: Vec<W>,
    bytes: usize,
}

impl<W> Default for Batch<W> {
    fn default() -> Self {
        Self {
            ops: Vec::new(),
            stamps: Vec::new(),
            waiters: Vec::new(),
            bytes: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Putting<W> {
    seq: u64,
    batch: Arc<WalBatch>,
    stamps: Vec<u64>,
    waiters: Vec<W>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum State<W> {
    Open {
        next_seq: u64,
        pending: Batch<W>,
        putting: Option<Putting<W>>,
        /// The pending batch should go out as soon as allowed.
        due: bool,
        closing: bool,
    },
    Stopped(Stopped),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogWriter<W> {
    writer: WriterId,
    max_bytes: usize,
    state: State<W>,
}

impl<W> LogWriter<W> {
    pub fn new(writer: WriterId, max_bytes: usize) -> Self {
        Self {
            writer,
            max_bytes,
            state: State::Open {
                next_seq: 1,
                pending: Batch::default(),
                putting: None,
                due: false,
                closing: false,
            },
        }
    }

    /// Nothing pending or being written: a closing driver can exit.
    pub fn idle(&self) -> bool {
        match &self.state {
            State::Open {
                pending, putting, ..
            } => pending.ops.is_empty() && putting.is_none(),
            State::Stopped(_) => true,
        }
    }

    /// Writes wait for the next flush tick. Ticks do nothing otherwise.
    pub fn has_pending(&self) -> bool {
        matches!(&self.state, State::Open { pending, .. } if !pending.ops.is_empty())
    }

    /// A due batch waits only for backpressure to ease.
    pub fn held_back(&self) -> bool {
        matches!(
            &self.state,
            State::Open { pending, putting: None, due: true, closing: false, .. } if !pending.ops.is_empty()
        )
    }

    pub fn step(&mut self, event: Event<W>, gate: Gate) -> Vec<Effect<W>> {
        let mut out = Vec::new();
        match (&mut self.state, event) {
            (State::Stopped(why), Event::Write { waiter, .. }) => {
                out.extend(waiter.map(|w| Effect::Ack {
                    waiters: vec![w],
                    result: Err(*why),
                }))
            }
            (State::Stopped(why), Event::Flush { waiter }) => out.push(Effect::Ack {
                waiters: vec![waiter],
                result: Err(*why),
            }),
            // A late completion after stopping can't happen (one put at a
            // time, and stopping only follows its completion); the rest
            // need nothing once stopped.
            (
                State::Stopped(_),
                Event::Tick | Event::Close | Event::Retry | Event::PutDone { .. },
            ) => {}
            (State::Open { pending, due, .. }, Event::Write { hlc, ops, waiter }) => {
                pending.bytes += ops
                    .iter()
                    .map(|(k, v)| entry_bytes(k, v.msg.encoded_len()))
                    .sum::<usize>();
                pending.ops.extend(ops);
                pending.stamps.push(hlc);
                pending.waiters.extend(waiter);
                *due |= pending.bytes >= self.max_bytes;
            }
            (
                State::Open {
                    pending,
                    putting,
                    due,
                    ..
                },
                Event::Flush { waiter },
            ) => match (pending.ops.is_empty(), putting) {
                (false, _) => {
                    pending.waiters.push(waiter);
                    *due = true;
                }
                (true, Some(p)) => p.waiters.push(waiter),
                (true, None) => out.push(Effect::Ack {
                    waiters: vec![waiter],
                    result: Ok(()),
                }),
            },
            (State::Open { pending, due, .. }, Event::Tick) => *due |= !pending.ops.is_empty(),
            (State::Open { due, closing, .. }, Event::Close) => {
                *due = true;
                *closing = true;
            }
            (State::Open { .. }, Event::Retry) => {}
            (
                State::Open {
                    next_seq, putting, ..
                },
                Event::PutDone { seq, outcome },
            ) => {
                let Some(p) = putting.take_if(|p| p.seq == seq) else {
                    // Not the put in flight: drivers send each outcome once.
                    return out;
                };
                match outcome {
                    PutOutcome::Written => {
                        *next_seq += 1;
                        let writes = p.stamps.len() as u64;
                        out.push(Effect::Durable {
                            seq,
                            batch: p.batch,
                            stamps: p.stamps,
                            writes,
                        });
                        out.push(Effect::Ack {
                            waiters: p.waiters,
                            result: Ok(()),
                        });
                    }
                    PutOutcome::Sealed | PutOutcome::Taken | PutOutcome::Failed => {
                        let why = match outcome {
                            PutOutcome::Sealed => Stopped::Sealed,
                            PutOutcome::Taken => Stopped::Taken,
                            PutOutcome::Failed | PutOutcome::Written => Stopped::Unknown,
                        };
                        self.stop(why, p, &mut out);
                        return out;
                    }
                }
            }
        }
        self.try_put(gate, &mut out);
        out
    }

    fn stop(&mut self, why: Stopped, failed: Putting<W>, out: &mut Vec<Effect<W>>) {
        let pending = match std::mem::replace(&mut self.state, State::Stopped(why)) {
            State::Open { pending, .. } => pending,
            State::Stopped(_) => Batch::default(),
        };
        out.push(Effect::Ack {
            waiters: failed.waiters.into_iter().chain(pending.waiters).collect(),
            result: Err(why),
        });
        out.push(Effect::Stopped {
            why,
            stamps: failed.stamps.into_iter().chain(pending.stamps).collect(),
        });
    }

    /// Starts the next put if one is due and allowed.
    fn try_put(&mut self, gate: Gate, out: &mut Vec<Effect<W>>) {
        let State::Open {
            next_seq,
            pending,
            putting,
            due,
            closing,
        } = &mut self.state
        else {
            return;
        };
        let allowed = *closing || gate == Gate::Open;
        if putting.is_some() || !*due || pending.ops.is_empty() || !allowed {
            return;
        }
        let batch = std::mem::take(pending);
        let wal = Arc::new(WalBatch {
            writer: self.writer,
            ops: batch.ops,
        });
        *putting = Some(Putting {
            seq: *next_seq,
            batch: wal.clone(),
            stamps: batch.stamps,
            waiters: batch.waiters,
        });
        *due = false;
        out.push(Effect::Put {
            seq: *next_seq,
            batch: wal,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Ts;
    use crate::types::Msg;

    fn write(hlc: u64, waiter: u32) -> Event<u32> {
        let v = Version {
            ts: Ts {
                hlc,
                writer: WriterId(1),
            },
            msg: Msg::Put(Bytes::from_static(b"v")),
        };
        Event::Write {
            hlc,
            ops: vec![(Bytes::from(format!("k{hlc}")), v)],
            waiter: Some(waiter),
        }
    }

    fn puts(effects: &[Effect<u32>]) -> Vec<u64> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Put { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect()
    }

    fn acked(effects: &[Effect<u32>]) -> Vec<(Vec<u32>, Result<(), Stopped>)> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Ack { waiters, result } => Some((waiters.clone(), *result)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn batches_on_tick_one_put_at_a_time_and_acks_when_durable() {
        let mut log = LogWriter::new(WriterId(1), 1 << 20);
        assert!(log.step(write(1, 10), Gate::Open).is_empty());
        let e = log.step(Event::Tick, Gate::Open);
        assert_eq!(puts(&e), [1]);
        // Writes during the put join the next batch.
        assert!(log.step(write(2, 11), Gate::Open).is_empty());
        assert!(puts(&log.step(Event::Tick, Gate::Open)).is_empty());
        let e = log.step(
            Event::PutDone {
                seq: 1,
                outcome: PutOutcome::Written,
            },
            Gate::Open,
        );
        assert_eq!(acked(&e), [(vec![10], Ok(()))]);
        assert_eq!(puts(&e), [2], "the due batch goes out next");
        let e = log.step(
            Event::PutDone {
                seq: 2,
                outcome: PutOutcome::Written,
            },
            Gate::Open,
        );
        assert_eq!(acked(&e), [(vec![11], Ok(()))]);
        assert!(log.idle());
    }

    #[test]
    fn backpressure_holds_a_due_batch_until_retry_or_close() {
        let mut log = LogWriter::new(WriterId(1), 1 << 20);
        log.step(write(1, 10), Gate::Closed);
        assert!(puts(&log.step(Event::Tick, Gate::Closed)).is_empty());
        assert!(log.held_back());
        assert_eq!(puts(&log.step(Event::Retry, Gate::Open)), [1]);

        let mut log = LogWriter::new(WriterId(1), 1 << 20);
        log.step(write(1, 10), Gate::Closed);
        assert_eq!(
            puts(&log.step(Event::Close, Gate::Closed)),
            [1],
            "close ignores backpressure"
        );
    }

    #[test]
    fn a_seal_stops_the_log_and_fails_every_waiter() {
        let mut log = LogWriter::new(WriterId(1), 1 << 20);
        log.step(write(1, 10), Gate::Open);
        log.step(Event::Tick, Gate::Open);
        log.step(write(2, 11), Gate::Open);
        let e = log.step(
            Event::PutDone {
                seq: 1,
                outcome: PutOutcome::Sealed,
            },
            Gate::Open,
        );
        assert_eq!(acked(&e), [(vec![10, 11], Err(Stopped::Sealed))]);
        assert!(e.iter().any(
            |e| matches!(e, Effect::Stopped { why: Stopped::Sealed, stamps } if stamps == &[1, 2])
        ));
        let e = log.step(write(3, 12), Gate::Open);
        assert_eq!(acked(&e), [(vec![12], Err(Stopped::Sealed))]);
        assert!(log.idle());
    }

    #[test]
    fn flush_acks_after_everything_before_it() {
        let mut log = LogWriter::new(WriterId(1), 1 << 20);
        assert_eq!(
            acked(&log.step(Event::Flush { waiter: 1 }, Gate::Open)),
            [(vec![1], Ok(()))]
        );
        log.step(write(1, 10), Gate::Open);
        let e = log.step(Event::Flush { waiter: 2 }, Gate::Open);
        assert_eq!(puts(&e), [1]);
        let e = log.step(Event::Flush { waiter: 3 }, Gate::Open);
        assert!(acked(&e).is_empty(), "waits for the put in flight");
        let e = log.step(
            Event::PutDone {
                seq: 1,
                outcome: PutOutcome::Written,
            },
            Gate::Open,
        );
        assert_eq!(acked(&e), [(vec![10, 2, 3], Ok(()))]);
    }

    #[test]
    fn size_triggers_a_put_and_inflight_tracks_the_floor() {
        let mut log = LogWriter::new(WriterId(1), 1);
        assert_eq!(puts(&log.step(write(1, 10), Gate::Open)), [1]);
        let mut inflight = Inflight::default();
        inflight.register(5);
        inflight.register(5);
        inflight.register(9);
        assert_eq!(inflight.floor(100), 5);
        inflight.release(&[5]);
        assert_eq!(inflight.floor(100), 5);
        inflight.release(&[5, 9]);
        assert_eq!(inflight.floor(100), 100);
    }
}
