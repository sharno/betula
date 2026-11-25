//! A Stateright model of sidra's protocol that drives the crate's own state
//! machines (`sidra::protocol`): writer logs, leases, the tombstone bound,
//! log sealing, and the applier's checkpoint stages. One actor plays the
//! object store; the others are processes with independent clocks.
//!
//! The data plane is abstract: a checkpoint's tree is one segment object
//! holding the whole key -> version map.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use sidra::protocol::checkpoint::{self, Applied, Applying, Plan, Publishing, Scanned};
use sidra::protocol::follower::{self, Reading, View};
use sidra::protocol::log::{self, Gate, Inflight, LogWriter, PutOutcome};
use sidra::protocol::tombstones::Pull;
use sidra::protocol::{Candidacy, Lease, Manifest, Msg as Op, Version, WalBatch, WalRecord};
use sidra::{
    ApplierRole, CleanerConfig, Epoch, LogHome, ManifestId, NodePtr, SegmentId, TreeRoot, Ts,
    WriterId,
};
use stateright::{Model, Property};

// ---------------------------------------------------------------------------
// Configuration

/// What a process writes, in order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Write {
    Put(&'static str, &'static str),
    Delete(&'static str),
}

#[derive(Clone, Debug)]
pub struct Config {
    /// One script per process; process 0 holds the applier role.
    pub scripts: Vec<Vec<Write>>,
    /// Clock readings run from 1 to this.
    pub max_clock: u64,
    pub lease_ttl: u64,
    /// Checkpoints stop after this manifest id.
    pub max_manifest: u64,
    /// Writers move their clocks past the tombstone bound before writing
    /// (the protocol). `false` reproduces the bug that rule fixes.
    pub observe_bound: bool,
    /// Writers renew their leases each clock tick. Off, leases run out and
    /// the applier seals their logs, which also lets the floor rise.
    pub renew_leases: bool,
    /// Faults allowed per run: lost requests, lost responses, crashes.
    pub max_faults: u8,
}

// ---------------------------------------------------------------------------
// Object store

/// Object paths, ordered so a listing is a range scan.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Path {
    Manifest(u64),
    Lease(WriterId),
    Log(WriterId, u64),
    Segment(SegmentId),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Listing {
    Manifests,
    Leases,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Request {
    Get(Path),
    Put(Path, Vec<u8>),
    PutIfAbsent(Path, Vec<u8>),
    List(Listing),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Response {
    Got(Option<Vec<u8>>),
    Done,
    Created,
    Exists(Vec<u8>),
    Listed(Vec<Path>),
    /// The request failed after retries (whether it applied is unknown to
    /// the caller).
    Failed,
}

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_stdvec(value).expect("model objects serialize")
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> T {
    postcard::from_bytes(bytes).expect("model objects deserialize")
}

type Tree = BTreeMap<Bytes, Version>;

fn store_step(objects: &mut BTreeMap<Path, Vec<u8>>, req: Request) -> Response {
    match req {
        Request::Get(p) => Response::Got(objects.get(&p).cloned()),
        Request::Put(p, v) => {
            objects.insert(p, v);
            Response::Done
        }
        Request::PutIfAbsent(p, v) => match objects.get(&p) {
            Some(existing) => Response::Exists(existing.clone()),
            None => {
                objects.insert(p, v);
                Response::Created
            }
        },
        Request::List(Listing::Manifests) => Response::Listed(
            objects
                .keys()
                .filter(|p| matches!(p, Path::Manifest(_)))
                .cloned()
                .collect(),
        ),
        Request::List(Listing::Leases) => Response::Listed(
            objects
                .keys()
                .filter(|p| matches!(p, Path::Lease(_)))
                .cloned()
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Processes

/// A process's writer: starting (lease, then the manifest's bound), or
/// writing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Phase {
    NotStarted,
    /// Lease put sent.
    Leasing,
    /// Listing manifests, then reading the newest, to observe its bound.
    Observing,
    Ready,
}

/// One checkpoint in progress, one request at a time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Stage {
    Idle,
    ListingLeases,
    GettingLeases {
        pending: Vec<WriterId>,
        got: Vec<Lease>,
        present: BTreeSet<WriterId>,
    },
    Pulling {
        scanned: ScannedBox,
        queue: Vec<(WriterId, u64)>,
        current: Option<(WriterId, Reading)>,
    },
    Sealing {
        sealing: checkpoint::Sealing,
        queue: Vec<WriterId>,
        current: Option<(WriterId, follower::Sealing)>,
    },
    LoadingTree(Applying),
    PuttingSegment {
        applying: Applying,
        seg: SegmentId,
    },
    Publishing(Publishing),
}

/// `Scanned` boxed so `Stage` stays small.
type ScannedBox = Box<Scanned>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Applier {
    epoch: Epoch,
    next_seq: u64,
    view: View,
    tracked: follower::Tracked,
    stage: Stage,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Proc {
    me: WriterId,
    clock: u64,
    /// Hybrid clock: the last timestamp handed out or observed.
    hlc: u64,
    inflight: Inflight,
    phase: Phase,
    log: LogWriter<usize>,
    script_pos: usize,
    /// Writes handed to the log, by script position, until acknowledged.
    pending: BTreeMap<usize, (Bytes, Version)>,
    /// Acknowledged writes: these must stay visible (unless overwritten by
    /// newer versions).
    pub acked: Vec<(Bytes, Version)>,
    stopped: bool,
    /// Crashed: takes no more steps; requests it sent may still land.
    crashed: bool,
    /// Clock reading of the last lease written: one renewal per tick.
    leased_at: u64,
    /// Checkpoints started (bounded, so the state space is finite).
    attempts: u64,
    applier: Option<Applier>,
    /// Requests sent, not yet delivered. Delivered in any order.
    outbox: Vec<(Purpose, Request)>,
    timers: BTreeSet<Timer>,
}

/// What a store request was for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Purpose {
    Lease,
    LogPut(u64, Arc<WalBatch>),
    ListManifests,
    GetManifest(u64),
    Checkpoint,
}

impl Proc {
    fn hlc_now(&mut self) -> u64 {
        self.hlc = self.clock.max(self.hlc + 1);
        self.hlc
    }

    /// The lease floor: a fresh hybrid reading, not stored. A later write
    /// may then share its value, which is safe: tombstones are dropped
    /// strictly below the floor.
    fn floor(&self) -> u64 {
        self.inflight.floor(self.clock.max(self.hlc + 1))
    }

    fn lease(&mut self, cfg: &Config) -> Lease {
        self.leased_at = self.clock;
        Lease {
            writer: self.me,
            log: LogHome::Home,
            candidacy: Candidacy::Primary,
            floor_hlc: self.floor(),
            expires_at_ms: self.clock + cfg.lease_ttl,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Timer {
    Clock,
    Start,
    Write,
    Flush,
    Lease,
    Checkpoint,
}

fn writer_of(index: usize) -> WriterId {
    WriterId(index as u64 + 1)
}

fn initial_manifest() -> Manifest {
    Manifest {
        applier_epoch: Epoch(1),
        applier: Some(writer_of(0)),
        ..Manifest::initial(0)
    }
}

/// Timers a step arms (a process's requests go straight to its outbox).
#[derive(Default)]
pub struct Out {
    timers: Vec<Timer>,
}

fn arm(o: &mut Out, timer: Timer) {
    o.timers.push(timer);
}

impl Proc {
    /// Queues a store request; the model delivers it (atomically with its
    /// response) whenever it chooses.
    fn send(&mut self, _o: &mut Out, purpose: Purpose, req: Request) {
        self.outbox.push((purpose, req));
    }

    /// Runs log effects: puts go to the store, acks mark writes acknowledged.
    fn log_effects(&mut self, effects: Vec<log::Effect<usize>>, o: &mut Out) {
        for e in effects {
            match e {
                log::Effect::Put { seq, batch } => {
                    let record = encode(&WalRecord::Batch(batch.as_ref().clone()));
                    self.send(
                        o,
                        Purpose::LogPut(seq, batch),
                        Request::PutIfAbsent(Path::Log(self.me, seq), record),
                    );
                }
                log::Effect::Durable {
                    seq, batch, stamps, ..
                } => {
                    self.inflight.release(&stamps);
                    let clock = self.clock;
                    if let Some(a) = &mut self.applier {
                        a.view.add_own(seq, batch, clock);
                    }
                }
                log::Effect::Ack { waiters, result } => waiters.into_iter().for_each(|w| {
                    if let (Some(write), Ok(())) = (self.pending.remove(&w), result) {
                        self.acked.push(write);
                    }
                }),
                log::Effect::Stopped { stamps, .. } => {
                    self.inflight.release(&stamps);
                    self.stopped = true;
                }
            }
        }
    }
}

/// A process as it starts: its clock at 1 and its start timers armed.
fn start(index: usize) -> Proc {
    let me = writer_of(index);
    let mut timers: BTreeSet<Timer> = [Timer::Clock, Timer::Start].into();
    if index == 0 {
        timers.insert(Timer::Checkpoint);
    }
    Proc {
        me,
        clock: 1,
        hlc: 0,
        inflight: Inflight::default(),
        phase: Phase::NotStarted,
        // Each write is its own batch (put at once): batching only
        // multiplies interleavings, not behaviours.
        log: LogWriter::new(me, 1),
        script_pos: 0,
        pending: BTreeMap::new(),
        acked: Vec::new(),
        stopped: false,
        crashed: false,
        leased_at: 0,
        attempts: 0,
        applier: (index == 0).then(|| Applier {
            epoch: Epoch(1),
            next_seq: 0,
            view: View {
                manifest_id: ManifestId(1),
                manifest: Arc::new(initial_manifest()),
                tails: follower::Tails::default(),
            },
            tracked: follower::Tracked::default(),
            stage: Stage::Idle,
        }),
        outbox: Vec::new(),
        timers,
    }
}

/// Whether firing `timer` would change anything (skipped otherwise, so it
/// adds no states).
fn useful(p: &Proc, cfg: &Config, index: usize, timer: &Timer) -> bool {
    match timer {
        Timer::Clock => p.clock < cfg.max_clock,
        // A process with nothing to write never opens a writer.
        Timer::Start => p.phase == Phase::NotStarted && !cfg.scripts[index].is_empty(),
        Timer::Write => {
            p.phase == Phase::Ready && !p.stopped && p.script_pos < cfg.scripts[index].len()
        }
        Timer::Flush => !p.log.idle(),
        Timer::Lease => {
            cfg.renew_leases && p.phase == Phase::Ready && !p.stopped && p.clock > p.leased_at
        }
        Timer::Checkpoint => {
            p.attempts < cfg.max_manifest
                && p.applier.as_ref().is_some_and(|a| {
                    a.stage == Stage::Idle && a.view.manifest_id.0 < cfg.max_manifest
                })
        }
    }
}

fn fire(p: &mut Proc, cfg: &Config, index: usize, timer: &Timer, o: &mut Out) {
    match timer {
        Timer::Clock => {
            p.clock += 1;
            arm(o, Timer::Clock);
            arm(o, Timer::Lease);
        }
        Timer::Start => {
            p.phase = Phase::Leasing;
            let lease = p.lease(cfg);
            p.send(
                o,
                Purpose::Lease,
                Request::Put(Path::Lease(p.me), encode(&lease)),
            );
        }
        Timer::Write => {
            let (key, msg) = match &cfg.scripts[index][p.script_pos] {
                Write::Put(k, v) => (*k, Op::Put(Bytes::from_static(v.as_bytes()))),
                Write::Delete(k) => (*k, Op::Delete),
            };
            let hlc = p.hlc_now();
            p.inflight.register(hlc);
            let version = Version {
                ts: Ts { hlc, writer: p.me },
                msg,
            };
            let key = Bytes::from_static(key.as_bytes());
            p.pending
                .insert(p.script_pos, (key.clone(), version.clone()));
            let waiter = p.script_pos;
            p.script_pos += 1;
            let effects = p.log.step(
                log::Event::Write {
                    hlc,
                    ops: vec![(key, version)],
                    waiter: Some(waiter),
                },
                Gate::Open,
            );
            p.log_effects(effects, o);
            arm(o, Timer::Write);
        }
        Timer::Flush => {
            let effects = p.log.step(log::Event::Tick, Gate::Open);
            p.log_effects(effects, o);
            arm(o, Timer::Flush);
        }
        Timer::Lease => {
            let lease = p.lease(cfg);
            p.send(
                o,
                Purpose::Lease,
                Request::Put(Path::Lease(p.me), encode(&lease)),
            );
        }
        Timer::Checkpoint => {
            p.attempts += 1;
            if let Some(a) = &mut p.applier {
                a.stage = Stage::ListingLeases;
            }
            p.send(o, Purpose::Checkpoint, Request::List(Listing::Leases));
        }
    }
}

fn on_response(p: &mut Proc, cfg: &Config, purpose: Purpose, resp: Response, o: &mut Out) {
    match (purpose, resp) {
        // Opening fails: the writer gives up (a new process would get a new
        // id).
        (Purpose::Lease | Purpose::ListManifests | Purpose::GetManifest(_), Response::Failed)
            if p.phase != Phase::Ready =>
        {
            p.stopped = true;
        }
        (Purpose::Lease, _) if p.phase == Phase::Leasing => {
            p.phase = Phase::Observing;
            p.send(o, Purpose::ListManifests, Request::List(Listing::Manifests));
        }
        (Purpose::Lease, _) => {}
        (Purpose::ListManifests, Response::Listed(paths)) => {
            let newest = paths
                .iter()
                .filter_map(|p| match p {
                    Path::Manifest(id) => Some(*id),
                    _ => None,
                })
                .max()
                .unwrap_or(0);
            p.send(
                o,
                Purpose::GetManifest(newest),
                Request::Get(Path::Manifest(newest)),
            );
        }
        (Purpose::GetManifest(_), Response::Got(found)) => {
            if cfg.observe_bound
                && let Some(bytes) = found
            {
                let m: Manifest = decode(&bytes);
                p.hlc = p.hlc.max(m.tombstone_bound);
            }
            p.phase = Phase::Ready;
            [Timer::Write, Timer::Lease]
                .into_iter()
                .for_each(|t| arm(o, t));
        }
        (Purpose::LogPut(seq, batch), resp) => {
            let outcome = match resp {
                Response::Created => PutOutcome::Written,
                Response::Exists(bytes) => match decode::<WalRecord>(&bytes) {
                    WalRecord::Seal => PutOutcome::Sealed,
                    WalRecord::Batch(b) if b == *batch => PutOutcome::Written,
                    WalRecord::Batch(_) => PutOutcome::Taken,
                },
                _ => PutOutcome::Failed,
            };
            let effects = p.log.step(log::Event::PutDone { seq, outcome }, Gate::Open);
            p.log_effects(effects, o);
        }
        (Purpose::Checkpoint, resp) => checkpoint_step(p, cfg, resp, o),
        (Purpose::ListManifests | Purpose::GetManifest(_), _) => {}
    }
}

/// Advances the applier's checkpoint by one store response.
fn checkpoint_step(p: &mut Proc, cfg: &Config, resp: Response, o: &mut Out) {
    let (me, clock) = (p.me, p.clock);
    let own_floor = p.floor();
    let Some(mut a) = p.applier.take() else {
        return;
    };
    let cleaner = CleanerConfig::default();
    let who = checkpoint::Applier {
        me,
        epoch: a.epoch,
        role: ApplierRole::Auto {
            steal_after: std::time::Duration::from_secs(3600),
        },
        cleaner: &cleaner,
        // The model's tree has no tombstones to purge.
        purge_after: std::time::Duration::from_secs(60),
    };
    let stage = std::mem::replace(&mut a.stage, Stage::Idle);
    let next = |req: Request, stage: Stage, a: &mut Applier, p: &mut Proc, o: &mut Out| {
        a.stage = stage;
        p.send(o, Purpose::Checkpoint, req);
    };
    match (stage, resp) {
        (Stage::ListingLeases, Response::Listed(paths)) => {
            let writers: Vec<WriterId> = paths
                .iter()
                .filter_map(|p| match p {
                    Path::Lease(w) => Some(*w),
                    _ => None,
                })
                .collect();
            a.tracked.track(writers.iter().copied());
            let present = writers.iter().copied().collect();
            advance_leases(&mut a, p, o, writers, Vec::new(), present, own_floor);
        }
        (
            Stage::GettingLeases {
                pending,
                mut got,
                present,
            },
            Response::Got(found),
        ) => {
            got.extend(found.map(|b| decode::<Lease>(&b)));
            advance_leases(&mut a, p, o, pending, got, present, own_floor);
        }
        (
            Stage::Pulling {
                scanned,
                queue,
                current: Some((w, reading)),
            },
            Response::Got(found),
        ) => {
            let record = found.map(|b| decode::<WalRecord>(&b));
            match reading.record(record) {
                Err(more) => {
                    let seq = more.next();
                    next(
                        Request::Get(Path::Log(w, seq)),
                        Stage::Pulling {
                            scanned,
                            queue,
                            current: Some((w, more)),
                        },
                        &mut a,
                        p,
                        o,
                    );
                }
                Ok(read) => {
                    a.view.absorb(w, read, clock);
                    advance_pull(&mut a, p, o, *scanned, queue, me, clock);
                }
            }
        }
        (
            Stage::Sealing {
                sealing,
                queue,
                current: Some((w, s)),
            },
            resp,
        ) => {
            let s = match (s, resp) {
                (s @ follower::Sealing::Reading(_), Response::Got(found)) => {
                    s.record(found.map(|b| decode::<WalRecord>(&b)))
                }
                (s @ follower::Sealing::Placing { .. }, Response::Created) => s.sealed(),
                (s @ follower::Sealing::Placing { .. }, Response::Exists(bytes)) => {
                    match decode(&bytes) {
                        WalRecord::Batch(b) => s.lost(b),
                        WalRecord::Seal => s.sealed(),
                    }
                }
                (s, _) => s,
            };
            advance_seal(&mut a, p, o, sealing, queue, Some((w, s)), clock, who);
        }
        (Stage::LoadingTree(applying), Response::Got(found)) => {
            let mut tree: Tree = found.map(|b| decode(&b)).unwrap_or_default();
            applying.msgs.iter().for_each(|(k, v)| {
                let newer = tree.get(k).is_none_or(|old| v.ts > old.ts);
                if newer {
                    tree.insert(k.clone(), v.clone());
                }
            });
            tree.retain(|_, v| !(matches!(v.msg, Op::Delete) && v.ts < applying.drop_below));
            let seg = SegmentId {
                epoch: a.epoch,
                seq: a.next_seq,
            };
            a.next_seq += 1;
            next(
                Request::Put(Path::Segment(seg), encode(&tree)),
                Stage::PuttingSegment { applying, seg },
                &mut a,
                p,
                o,
            );
        }
        (Stage::PuttingSegment { applying, seg }, Response::Done) => {
            let done = Applied {
                tree: Some(TreeRoot {
                    ptr: NodePtr {
                        seg,
                        offset: 0,
                        len: 0,
                    },
                    purge_at: None,
                    newest: None,
                    height: 0,
                }),
                segments: Default::default(),
                dead: Vec::new(),
                segment_watermark: SegmentId {
                    epoch: a.epoch,
                    seq: a.next_seq,
                },
            };
            let publishing = applying.applied(who, done, clock);
            let id = a.view.manifest_id.next();
            next(
                Request::PutIfAbsent(Path::Manifest(id.0), encode(&publishing.manifest)),
                Stage::Publishing(publishing),
                &mut a,
                p,
                o,
            );
        }
        (Stage::Publishing(publishing), Response::Created) => {
            let id = a.view.manifest_id.next();
            a.view.install(id, Arc::new(publishing.manifest), clock);
            a.stage = Stage::Idle;
            let _ = cfg;
            arm(o, Timer::Checkpoint);
        }
        // Anything else (a lost publish race can't happen with one
        // applier) ends the checkpoint.
        (_, _) => {
            a.stage = Stage::Idle;
            arm(o, Timer::Checkpoint);
        }
    }
    p.applier = Some(a);
}

#[allow(clippy::too_many_arguments)]
fn advance_leases(
    a: &mut Applier,
    p: &mut Proc,
    o: &mut Out,
    mut pending: Vec<WriterId>,
    got: Vec<Lease>,
    present: BTreeSet<WriterId>,
    own_floor: u64,
) {
    match pending.pop() {
        Some(w) => {
            a.stage = Stage::GettingLeases {
                pending,
                got,
                present,
            };
            p.send(o, Purpose::Checkpoint, Request::Get(Path::Lease(w)));
        }
        None => {
            let scanned = Scanned {
                own_floor,
                leases: got,
                present,
            };
            let m = a.view.manifest.clone();
            let queue = a
                .view
                .reads(&m, &a.tracked, Some(p.me), follower::Scope::AllWriters);
            let (me, clock) = (p.me, p.clock);
            advance_pull(a, p, o, scanned, queue, me, clock);
        }
    }
}

fn advance_pull(
    a: &mut Applier,
    p: &mut Proc,
    o: &mut Out,
    scanned: Scanned,
    mut queue: Vec<(WriterId, u64)>,
    me: WriterId,
    clock: u64,
) {
    match queue.pop() {
        Some((w, from)) => {
            a.stage = Stage::Pulling {
                scanned: Box::new(scanned),
                queue,
                current: Some((w, Reading::from(from))),
            };
            p.send(o, Purpose::Checkpoint, Request::Get(Path::Log(w, from)));
        }
        None => {
            let sealing = scanned.pulled(&a.view, me, clock, Pull::Complete);
            let queue = sealing.to_seal.clone();
            let cleaner = CleanerConfig::default();
            let who = checkpoint::Applier {
                me,
                epoch: a.epoch,
                role: ApplierRole::Auto {
                    steal_after: std::time::Duration::from_secs(3600),
                },
                cleaner: &cleaner,
                purge_after: std::time::Duration::from_secs(60),
            };
            advance_seal(a, p, o, sealing, queue, None, clock, who);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_seal(
    a: &mut Applier,
    p: &mut Proc,
    o: &mut Out,
    sealing: checkpoint::Sealing,
    mut queue: Vec<WriterId>,
    current: Option<(WriterId, follower::Sealing)>,
    clock: u64,
    who: checkpoint::Applier,
) {
    let current = match current {
        Some((w, follower::Sealing::Done(read))) => {
            a.view.absorb(w, read, clock);
            None
        }
        other => other,
    };
    let current = current.or_else(|| {
        queue.pop().map(|w| {
            let from = a.view.tails.next_seq(w, &a.view.manifest.frontier);
            (w, follower::Sealing::from(from))
        })
    });
    match current {
        Some((w, s)) => {
            let req = match &s {
                follower::Sealing::Reading(r) => Request::Get(Path::Log(w, r.next())),
                follower::Sealing::Placing { at, .. } => {
                    Request::PutIfAbsent(Path::Log(w, *at), encode(&WalRecord::Seal))
                }
                follower::Sealing::Done(_) => unreachable!("absorbed above"),
            };
            a.stage = Stage::Sealing {
                sealing,
                queue,
                current: Some((w, s)),
            };
            p.send(o, Purpose::Checkpoint, req);
        }
        None => match sealing.sealed(&a.view, who, clock) {
            Plan::Nothing => {
                a.stage = Stage::Idle;
                arm(o, Timer::Checkpoint);
            }
            Plan::Apply(applying) => {
                let req = match a.view.manifest.tree {
                    Some(root) => Request::Get(Path::Segment(root.ptr.seg)),
                    None => Request::Get(Path::Segment(SegmentId {
                        epoch: Epoch(0),
                        seq: u64::MAX,
                    })),
                };
                a.stage = Stage::LoadingTree(applying);
                p.send(o, Purpose::Checkpoint, req);
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Properties

/// What a fresh reader sees for every key: the newest manifest's tree plus
/// every log entry past its frontier (up to the first gap or seal).
pub fn visible(objects: &BTreeMap<Path, Vec<u8>>) -> Tree {
    let Some((_, m)) = objects
        .iter()
        .filter_map(|(p, b)| match p {
            Path::Manifest(id) => Some((*id, decode::<Manifest>(b))),
            _ => None,
        })
        .max_by_key(|(id, _)| *id)
    else {
        return Tree::new();
    };
    let mut tree: Tree = m
        .tree
        .and_then(|r| objects.get(&Path::Segment(r.ptr.seg)))
        .map(|b| decode(b))
        .unwrap_or_default();
    let writers: BTreeSet<WriterId> = objects
        .keys()
        .filter_map(|p| match p {
            Path::Log(w, _) => Some(*w),
            _ => None,
        })
        .collect();
    for w in writers {
        let mut seq = m.frontier.get(&w).copied().unwrap_or(0) + 1;
        while let Some(WalRecord::Batch(b)) = objects.get(&Path::Log(w, seq)).map(|b| decode(b)) {
            b.ops.into_iter().for_each(|(k, v)| {
                if tree.get(&k).is_none_or(|old| v.ts > old.ts) {
                    tree.insert(k, v);
                }
            });
            seq += 1;
        }
    }
    tree
}

/// Newest version per key among `writes`.
fn newest<'a>(
    writes: impl Iterator<Item = (&'a Bytes, &'a Version)>,
) -> BTreeMap<&'a Bytes, &'a Version> {
    writes.fold(BTreeMap::new(), |mut m, (k, v)| {
        if m.get(k).is_none_or(|old: &&Version| v.ts > old.ts) {
            m.insert(k, v);
        }
        m
    })
}

/// Whether `seen` holds nothing older than `expected`: a put must show (or
/// something newer), a delete may show as absence (or something newer).
/// An older version showing is a lost write or a resurrected key.
fn covers(seen: &Tree, expected: &BTreeMap<&Bytes, &Version>) -> bool {
    expected.iter().all(|(k, a)| match (&a.msg, seen.get(*k)) {
        (_, Some(now)) => now.ts >= a.ts,
        (Op::Delete, None) => true,
        (Op::Put(_), None) => false,
    })
}

/// The whole system: the object store and every process.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sys {
    pub store: BTreeMap<Path, Vec<u8>>,
    pub procs: Vec<Proc>,
    pub faults: u8,
}

/// A step: deliver one of a process's requests (the store applies it and
/// the process handles the response, as one step), or fire one of its
/// timers. Handling a response touches only the process's own state, so it
/// commutes with other processes' steps: fusing the two loses no behaviour
/// (partial-order reduction) and cuts the state space by orders of
/// magnitude.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Deliver {
        proc: usize,
        request: usize,
        fate: Fate,
    },
    Fire {
        proc: usize,
        timer: Timer,
    },
    Crash {
        proc: usize,
    },
}

/// What happens to a delivered request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fate {
    Ok,
    /// Lost before reaching the store.
    Dropped,
    /// Applied, but the response is lost.
    ResponseLost,
}

pub struct Checked {
    pub cfg: Config,
}

/// Every acknowledged write is visible or superseded.
pub fn acknowledged_writes_stay_visible(_: &Checked, s: &Sys) -> bool {
    let acked = newest(
        s.procs
            .iter()
            .flat_map(|p| p.acked.iter().map(|(k, v)| (k, v))),
    );
    covers(&visible(&s.store), &acked)
}

/// Every batch durable in a log stays visible or superseded, acknowledged
/// or not: a manifest must not count an entry as applied that its tree
/// lacks.
pub fn durable_writes_stay_visible(_: &Checked, s: &Sys) -> bool {
    let batches: Vec<WalBatch> = s
        .store
        .iter()
        .filter(|(p, _)| matches!(p, Path::Log(..)))
        .filter_map(|(_, b)| match decode::<WalRecord>(b) {
            WalRecord::Batch(batch) => Some(batch),
            WalRecord::Seal => None,
        })
        .collect();
    let durable = newest(
        batches
            .iter()
            .flat_map(|b| b.ops.iter().map(|(k, v)| (k, v))),
    );
    covers(&visible(&s.store), &durable)
}

/// An acknowledged delete's tombstone got dropped (the properties aren't
/// vacuous).
fn a_tombstone_gets_dropped(_: &Checked, s: &Sys) -> bool {
    let seen = visible(&s.store);
    s.procs
        .iter()
        .flat_map(|p| &p.acked)
        .any(|(k, v)| matches!(v.msg, Op::Delete) && !seen.contains_key(k))
}

impl Model for Checked {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Sys> {
        vec![Sys {
            store: [(Path::Manifest(1), encode(&initial_manifest()))].into(),
            procs: (0..self.cfg.scripts.len()).map(start).collect(),
            faults: 0,
        }]
    }

    fn actions(&self, s: &Sys, actions: &mut Vec<Action>) {
        let faulty = s.faults < self.cfg.max_faults;
        let fates: &[Fate] = match faulty {
            true => &[Fate::Ok, Fate::Dropped, Fate::ResponseLost],
            false => &[Fate::Ok],
        };
        s.procs.iter().enumerate().for_each(|(proc, p)| {
            actions.extend((0..p.outbox.len()).flat_map(|request| {
                fates.iter().map(move |&fate| Action::Deliver {
                    proc,
                    request,
                    fate,
                })
            }));
            if p.crashed {
                return;
            }
            actions.extend(
                p.timers
                    .iter()
                    .filter(|t| useful(p, &self.cfg, proc, t))
                    .map(|t| Action::Fire {
                        proc,
                        timer: t.clone(),
                    }),
            );
            if faulty {
                actions.push(Action::Crash { proc });
            }
        });
    }

    fn next_state(&self, s: &Sys, action: Action) -> Option<Sys> {
        let mut s = s.clone();
        let mut o = Out::default();
        match action {
            Action::Deliver {
                proc,
                request,
                fate,
            } => {
                let (purpose, req) = s.procs[proc].outbox.remove(request);
                let resp = match fate {
                    Fate::Ok => store_step(&mut s.store, req),
                    Fate::Dropped => Response::Failed,
                    Fate::ResponseLost => {
                        store_step(&mut s.store, req);
                        Response::Failed
                    }
                };
                if fate != Fate::Ok {
                    s.faults += 1;
                }
                let p = &mut s.procs[proc];
                if !p.crashed {
                    on_response(p, &self.cfg, purpose, resp, &mut o);
                    p.timers.extend(o.timers);
                }
            }
            Action::Crash { proc } => {
                s.faults += 1;
                let p = &mut s.procs[proc];
                p.crashed = true;
                p.timers.clear();
            }
            Action::Fire { proc, timer } => {
                let p = &mut s.procs[proc];
                p.timers.remove(&timer);
                fire(p, &self.cfg, proc, &timer, &mut o);
                p.timers.extend(o.timers);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "acknowledged writes stay visible",
                acknowledged_writes_stay_visible,
            ),
            Property::always("durable writes stay visible", durable_writes_stay_visible),
            Property::sometimes("a tombstone gets dropped", a_tombstone_gets_dropped),
        ]
    }
}

/// Replaying hand-picked traces: each step must be enabled.
impl Checked {
    pub fn initial(&self) -> Sys {
        self.init_states().remove(0)
    }

    /// Fires `timer` on `proc`; panics if it isn't enabled.
    pub fn fire(&self, s: Sys, proc: usize, timer: Timer) -> Sys {
        let action = Action::Fire { proc, timer };
        let mut enabled = Vec::new();
        self.actions(&s, &mut enabled);
        assert!(enabled.contains(&action), "not enabled: {action:?}");
        self.next_state(&s, action).expect("a step")
    }

    /// Delivers `proc`'s oldest request with the given fate.
    pub fn deliver(&self, s: Sys, proc: usize, fate: Fate) -> Sys {
        let action = Action::Deliver {
            proc,
            request: 0,
            fate,
        };
        self.next_state(&s, action).expect("a step")
    }

    /// Crashes `proc`.
    pub fn crash(&self, s: Sys, proc: usize) -> Sys {
        self.next_state(&s, Action::Crash { proc }).expect("a step")
    }

    /// Delivers `proc`'s requests (and the ones they lead to) until it has
    /// none outstanding.
    pub fn drain(&self, mut s: Sys, proc: usize) -> Sys {
        while !s.procs[proc].outbox.is_empty() {
            s = self.deliver(s, proc, Fate::Ok);
        }
        s
    }
}

pub fn model(cfg: Config) -> Checked {
    Checked { cfg }
}

/// The scenario behind the tombstone-bound rule: an applier, a writer that
/// deletes a key, and one that starts late (possibly with a lagging clock)
/// and puts it. The deleter can't be the applier: the applier's clock must
/// pass the delete's timestamp to publish a bound above it.
pub fn late_writer(observe_bound: bool) -> Config {
    Config {
        // The deleter's later writes give the next checkpoints work: one
        // publishes a bound above the delete, the next drops its tombstone
        // (a checkpoint with nothing to apply rewrites nothing).
        scripts: vec![
            vec![],
            vec![
                Write::Delete("k"),
                Write::Put("x", "1"),
                Write::Put("y", "1"),
            ],
            vec![Write::Put("k", "b")],
        ],
        max_clock: 3,
        lease_ttl: 1,
        max_manifest: 4,
        observe_bound,
        renew_leases: true,
        max_faults: 0,
    }
}

/// Small enough to search exhaustively (~25M states, ~20 s, ~11 GB): the
/// applier also writes (a delete), and a second writer puts the same key.
pub fn small() -> Config {
    Config {
        scripts: vec![vec![Write::Delete("k")], vec![Write::Put("k", "b")]],
        max_clock: 3,
        lease_ttl: 1,
        max_manifest: 3,
        observe_bound: true,
        renew_leases: true,
        max_faults: 0,
    }
}

/// The delayed-write scenario: the applier deletes `k` (then writes more,
/// so later checkpoints have work); another writer's older put of `k` is
/// held in flight past its lease.
pub fn delayed_write() -> Config {
    Config {
        scripts: vec![
            vec![Write::Delete("k"), Write::Put("x", "1")],
            vec![Write::Put("k", "old")],
        ],
        max_clock: 3,
        lease_ttl: 1,
        max_manifest: 4,
        observe_bound: true,
        renew_leases: true,
        max_faults: 0,
    }
}

impl Sys {
    /// Writes `proc` had acknowledged.
    pub fn acked(&self, proc: usize) -> &[(Bytes, Version)] {
        &self.procs[proc].acked
    }
}
