//! A Stateright model of sidra's protocol that drives the crate's own state
//! machines (`sidra::protocol`): opening a writer (claiming the manifest,
//! fencing the log), the writer's log, partial checkpoints, and log GC. One
//! actor plays the object store; the others are processes with independent
//! clocks, each opening a writer that fences the one before.
//!
//! The data plane is abstract: a checkpoint's tree is one segment object
//! holding the whole key -> version map, and like a leaf it keeps no
//! tombstones.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use sidra::protocol::checkpoint::{self, Applied, Work};
use sidra::protocol::fence::{self, AfterPlace, AfterRead, Fenced};
use sidra::protocol::gc::deletable_wals;
use sidra::protocol::log::{self, Gate, LogWriter, PutOutcome};
use sidra::protocol::tail::Tail;
use sidra::protocol::{FenceAt, Manifest, Msg as Op, Version, WalBatch, WalRecord};
use sidra::{CleanerConfig, Epoch, ManifestId, NodePtr, SegmentId, TreeRoot, Ts, WalSeq};
use stateright::{Model, Property};

// ---------------------------------------------------------------------------
// Configuration

/// What a process writes, in order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Write {
    Put(&'static str, &'static str),
    Delete(&'static str),
}

/// Which applied log slots GC deletes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogGc {
    Off,
    /// The protocol: applied slots, except fences.
    KeepFences,
    /// Every applied slot: the bug keeping fences fixes.
    DeleteFences,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// One script per process. Each process opens a writer (fencing any
    /// open one), writes its script, and checkpoints.
    pub scripts: Vec<Vec<Write>>,
    /// Clock readings run from 1 to this.
    pub max_clock: u64,
    /// Checkpoints stop after this manifest id.
    pub max_manifest: u64,
    /// A new writer fences the log before writing (the protocol). `false`
    /// starts it writing at the first empty slot, unfenced.
    pub place_fence: bool,
    /// A new writer's clock observes the manifest's newest applied
    /// timestamp (the protocol). `false` reproduces the bug that fixes.
    pub observe_last_ts: bool,
    pub gc: LogGc,
    /// Faults allowed per run: lost requests, lost responses, crashes.
    pub max_faults: u8,
}

// ---------------------------------------------------------------------------
// Object store

/// Object paths, ordered so a listing is a range scan.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Path {
    Manifest(u64),
    Log(WalSeq),
    Segment(SegmentId),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Request {
    Get(Path),
    Put(Path, Vec<u8>),
    PutIfAbsent(Path, Vec<u8>),
    /// The newest manifest id (a LIST).
    LatestManifest,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Response {
    Got(Option<Vec<u8>>),
    Done,
    Created,
    Exists(Vec<u8>),
    Latest(Option<u64>),
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
        Request::LatestManifest => Response::Latest(
            objects
                .keys()
                .filter_map(|p| match p {
                    Path::Manifest(id) => Some(*id),
                    _ => None,
                })
                .max(),
        ),
    }
}

fn manifest_id(objects: &BTreeMap<Path, Vec<u8>>) -> Option<(u64, Manifest)> {
    objects
        .iter()
        .filter_map(|(p, b)| match p {
            Path::Manifest(id) => Some((*id, decode::<Manifest>(b))),
            _ => None,
        })
        .max_by_key(|(id, _)| *id)
}

// ---------------------------------------------------------------------------
// Processes

/// Opening a writer, one store request at a time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Opening {
    /// Listing manifests.
    Listing,
    /// Reading the newest (`None`: there is none yet).
    Loading(Option<u64>),
    /// Publishing the claimed manifest at this id.
    Claiming(u64, Box<Manifest>),
    Reading(Claimed, fence::Reading),
    Placing(Claimed, fence::Placing),
    /// Fenced; checking no newer writer claimed a manifest meanwhile.
    Checking(Claimed, Fenced),
}

/// The manifest this writer published on opening.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Claimed {
    id: ManifestId,
    manifest: Arc<Manifest>,
}

/// One checkpoint in progress.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Stage {
    Idle,
    LoadingTree(Work),
    PuttingSegment(Work, SegmentId),
    Publishing(Work, Box<Manifest>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Writer {
    epoch: Epoch,
    log: LogWriter<usize>,
    manifest_id: ManifestId,
    manifest: Arc<Manifest>,
    tail: Tail,
    next_segment: u64,
    stage: Stage,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Phase {
    NotStarted,
    Opening(Opening),
    Open(Box<Writer>),
    /// Fenced, superseded, or failed: it takes no more writes.
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Proc {
    clock: u64,
    /// Hybrid clock: the last timestamp handed out or observed.
    hlc: Ts,
    phase: Phase,
    script_pos: usize,
    /// Writes handed to the log, by script position, until acknowledged.
    pending: BTreeMap<usize, (Bytes, Version)>,
    /// Acknowledged writes: these must stay visible (unless overwritten by
    /// newer versions).
    pub acked: Vec<(Bytes, Version)>,
    /// Crashed: takes no more steps; requests it sent may still land.
    crashed: bool,
    /// Checkpoints started (bounded, so the state space is finite).
    attempts: u64,
    /// Requests sent, not yet delivered. Delivered in any order.
    outbox: Vec<(Purpose, Request)>,
    timers: Vec<Timer>,
}

/// What a store request was for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Purpose {
    Open,
    LogPut(WalSeq, Arc<WalBatch>),
    Checkpoint,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Timer {
    Clock,
    Start,
    Write,
    Checkpoint,
}

/// The crate's hybrid clock (`hlc::Hlc`), over a plain value: milliseconds
/// in the high bits, a counter in the low 16.
fn physical(clock: u64) -> Ts {
    Ts(clock << 16)
}

impl Proc {
    fn hlc_now(&mut self) -> Ts {
        self.hlc = physical(self.clock).max(Ts(self.hlc.0 + 1));
        self.hlc
    }

    fn observe(&mut self, ts: Ts) {
        self.hlc = self.hlc.max(ts);
    }

    fn send(&mut self, purpose: Purpose, req: Request) {
        self.outbox.push((purpose, req));
    }

    fn arm(&mut self, timer: Timer) {
        if !self.timers.contains(&timer) {
            self.timers.push(timer);
            self.timers.sort();
        }
    }

    fn writer(&mut self) -> Option<&mut Writer> {
        match &mut self.phase {
            Phase::Open(w) => Some(w),
            _ => None,
        }
    }

    /// Runs log effects: puts go to the store, durable batches join the
    /// tail, acks mark writes acknowledged.
    fn log_effects(&mut self, effects: Vec<log::Effect<usize>>) {
        effects.into_iter().for_each(|e| match e {
            log::Effect::Put { seq, batch } => {
                let record = encode(&WalRecord::Batch(batch.as_ref().clone()));
                self.send(
                    Purpose::LogPut(seq, batch),
                    Request::PutIfAbsent(Path::Log(seq), record),
                );
            }
            log::Effect::Durable { seq, batch, .. } => {
                let clock = self.clock;
                if let Some(w) = self.writer() {
                    w.tail.add(seq, batch, clock);
                }
            }
            log::Effect::Ack { waiters, result } => waiters.into_iter().for_each(|w| {
                if let (Some(write), Ok(())) = (self.pending.remove(&w), result) {
                    self.acked.push(write);
                }
            }),
            log::Effect::Stopped { .. } => self.phase = Phase::Stopped,
        })
    }
}

/// A process as it starts: its clock at 1 and its start timers armed.
fn start() -> Proc {
    Proc {
        clock: 1,
        hlc: Ts::MIN,
        phase: Phase::NotStarted,
        script_pos: 0,
        pending: BTreeMap::new(),
        acked: Vec::new(),
        crashed: false,
        attempts: 0,
        outbox: Vec::new(),
        timers: vec![Timer::Clock, Timer::Start],
    }
}

/// Whether firing `timer` would change anything (skipped otherwise, so it
/// adds no states).
fn useful(p: &Proc, cfg: &Config, index: usize, timer: &Timer) -> bool {
    match (timer, &p.phase) {
        (Timer::Clock, _) => p.clock < cfg.max_clock,
        // A process with nothing to write never opens a writer.
        (Timer::Start, Phase::NotStarted) => !cfg.scripts[index].is_empty(),
        (Timer::Write, Phase::Open(_)) => p.script_pos < cfg.scripts[index].len(),
        (Timer::Checkpoint, Phase::Open(w)) => {
            p.attempts < cfg.max_manifest
                && w.stage == Stage::Idle
                && w.manifest_id.0 < cfg.max_manifest
                && !w.tail.is_empty()
        }
        (Timer::Start | Timer::Write | Timer::Checkpoint, _) => false,
    }
}

fn fire(p: &mut Proc, cfg: &Config, index: usize, timer: &Timer) {
    match timer {
        Timer::Clock => {
            p.clock += 1;
            p.arm(Timer::Clock);
        }
        Timer::Start => {
            p.phase = Phase::Opening(Opening::Listing);
            p.send(Purpose::Open, Request::LatestManifest);
        }
        Timer::Write => {
            let (key, msg) = match &cfg.scripts[index][p.script_pos] {
                Write::Put(k, v) => (*k, Op::Put(Bytes::from_static(v.as_bytes()))),
                Write::Delete(k) => (*k, Op::Delete),
            };
            let version = Version {
                ts: p.hlc_now(),
                msg,
            };
            let key = Bytes::from_static(key.as_bytes());
            p.pending
                .insert(p.script_pos, (key.clone(), version.clone()));
            let waiter = p.script_pos;
            p.script_pos += 1;
            let Some(w) = p.writer() else { return };
            let effects = w.log.step(
                log::Event::Write {
                    ops: vec![(key, version)],
                    waiter: Some(waiter),
                },
                Gate::Open,
            );
            p.log_effects(effects);
            p.arm(Timer::Write);
        }
        Timer::Checkpoint => {
            p.attempts += 1;
            let now = p.hlc_now();
            let Some(w) = p.writer() else { return };
            // One batch per checkpoint, so backlogs span checkpoints.
            let work = checkpoint::plan(
                &w.tail,
                &w.manifest,
                1,
                &CleanerConfig::default(),
                Duration::from_secs(60),
                now,
            );
            let Some(work) = work else { return };
            let req = Request::Get(Path::Segment(match w.manifest.tree {
                Some(root) => root.ptr.seg,
                // Nothing stored here: an empty tree.
                None => SegmentId {
                    epoch: Epoch(0),
                    seq: u64::MAX,
                },
            }));
            w.stage = Stage::LoadingTree(work);
            p.send(Purpose::Checkpoint, req);
        }
    }
}

fn on_response(p: &mut Proc, cfg: &Config, purpose: Purpose, resp: Response) {
    match purpose {
        Purpose::Open => open_step(p, cfg, resp),
        Purpose::LogPut(seq, batch) => {
            let outcome = match resp {
                Response::Created => PutOutcome::Written,
                Response::Exists(bytes) => match decode::<WalRecord>(&bytes) {
                    WalRecord::Batch(b) if b == *batch => PutOutcome::Written,
                    WalRecord::Batch(_) | WalRecord::Fence(_) => PutOutcome::Fenced,
                },
                _ => PutOutcome::Failed,
            };
            let Some(w) = p.writer() else { return };
            let effects = w.log.step(log::Event::PutDone { seq, outcome }, Gate::Open);
            p.log_effects(effects);
        }
        Purpose::Checkpoint => checkpoint_step(p, resp),
    }
}

/// Advances opening by one store response. A failed request is resent.
fn open_step(p: &mut Proc, cfg: &Config, resp: Response) {
    let Phase::Opening(opening) = std::mem::replace(&mut p.phase, Phase::Stopped) else {
        return;
    };
    let clock = p.clock;
    let (next, req) = match (opening, resp) {
        (o, Response::Failed) => {
            let req = opening_request(&o);
            (o, req)
        }
        (Opening::Listing, Response::Latest(id)) => {
            let o = Opening::Loading(id);
            let req = opening_request(&o);
            (o, req)
        }
        (Opening::Loading(id), Response::Got(found)) => {
            let current = found.map_or_else(|| Manifest::initial(clock), |b| decode(&b));
            let o = Opening::Claiming(id.map_or(1, |id| id + 1), Box::new(current.claimed(clock)));
            let req = opening_request(&o);
            (o, req)
        }
        (Opening::Claiming(id, m), Response::Created) => {
            let claimed = Claimed {
                id: ManifestId(id),
                manifest: Arc::new(*m),
            };
            let reading =
                fence::Reading::new(claimed.manifest.writer_epoch, claimed.manifest.wal_applied);
            let o = Opening::Reading(claimed, reading);
            let req = opening_request(&o);
            (o, req)
        }
        // Another writer claimed it first, or one identical to ours (same
        // clock reading), which can't be told from our own lost response:
        // start over.
        (Opening::Claiming(..), Response::Exists(_)) => (Opening::Listing, Request::LatestManifest),
        (Opening::Reading(claimed, reading), Response::Got(found)) => {
            match reading.record(found.map(|b| decode(&b))) {
                Err(_) => return,
                Ok(AfterRead::More(r)) => {
                    let o = Opening::Reading(claimed, r);
                    let req = opening_request(&o);
                    (o, req)
                }
                Ok(AfterRead::Place(placing)) if !cfg.place_fence => {
                    // The bug: start writing at the empty slot, unfenced.
                    let at = placing.at();
                    let fenced = match placing.outcome(FenceAt::Placed) {
                        Ok(AfterPlace::Done(f)) => f,
                        _ => unreachable!("a placed fence is done"),
                    };
                    return opened(p, cfg, claimed, fenced, at);
                }
                Ok(AfterRead::Place(placing)) => {
                    let o = Opening::Placing(claimed, placing);
                    let req = opening_request(&o);
                    (o, req)
                }
            }
        }
        (Opening::Placing(claimed, placing), resp) => {
            let outcome = match resp {
                Response::Created => FenceAt::Placed,
                Response::Exists(bytes) => match decode::<WalRecord>(&bytes) {
                    WalRecord::Fence(e) if e == placing.epoch() => FenceAt::Placed,
                    record => FenceAt::Taken(record),
                },
                _ => unreachable!("a put-if-absent answers created or exists"),
            };
            match placing.outcome(outcome) {
                Err(_) => return,
                Ok(AfterPlace::Done(fenced)) => {
                    let o = Opening::Checking(claimed, fenced);
                    let req = opening_request(&o);
                    (o, req)
                }
                Ok(AfterPlace::More(r)) => {
                    let o = Opening::Reading(claimed, r);
                    let req = opening_request(&o);
                    (o, req)
                }
            }
        }
        (Opening::Checking(claimed, fenced), Response::Got(newer)) => {
            return match newer {
                Some(_) => {}
                None => {
                    let first = fenced.fence.next();
                    opened(p, cfg, claimed, fenced, first)
                }
            };
        }
        (o, resp) => unreachable!("{o:?} got {resp:?}"),
    };
    p.phase = Phase::Opening(next);
    p.send(Purpose::Open, req);
}

fn opening_request(o: &Opening) -> Request {
    match o {
        Opening::Listing => Request::LatestManifest,
        Opening::Loading(None) => Request::Get(Path::Manifest(0)),
        Opening::Loading(Some(id)) => Request::Get(Path::Manifest(*id)),
        Opening::Claiming(id, m) => Request::PutIfAbsent(Path::Manifest(*id), encode(m.as_ref())),
        Opening::Reading(_, r) => Request::Get(Path::Log(r.next())),
        Opening::Placing(_, pl) => {
            Request::PutIfAbsent(Path::Log(pl.at()), encode(&WalRecord::Fence(pl.epoch())))
        }
        Opening::Checking(c, _) => Request::Get(Path::Manifest(c.id.next().0)),
    }
}

/// The writer is open: replay what the fence read, set the clock past it,
/// and write from `first` on.
fn opened(p: &mut Proc, cfg: &Config, claimed: Claimed, fenced: Fenced, first: WalSeq) {
    let Fenced { read, fence } = fenced;
    read.batches
        .iter()
        .flat_map(|(_, b)| b.ops.iter().map(|(_, v)| v.ts))
        .chain(cfg.observe_last_ts.then_some(claimed.manifest.last_ts))
        .for_each(|ts| p.observe(ts));
    let mut tail = Tail::new(claimed.manifest.wal_applied);
    tail.absorb(read, p.clock);
    if cfg.place_fence {
        tail.add_fence(fence);
    }
    let epoch = claimed.manifest.writer_epoch;
    p.phase = Phase::Open(Box::new(Writer {
        epoch,
        // Each write is its own batch (put at once): batching only
        // multiplies interleavings, not behaviours.
        log: LogWriter::new(epoch, first, 1),
        manifest_id: claimed.id,
        manifest: claimed.manifest,
        tail,
        next_segment: 0,
        stage: Stage::Idle,
    }));
    p.arm(Timer::Write);
    p.arm(Timer::Checkpoint);
}

/// Advances the writer's checkpoint by one store response.
fn checkpoint_step(p: &mut Proc, resp: Response) {
    let clock = p.clock;
    let Some(w) = p.writer() else { return };
    let stage = std::mem::replace(&mut w.stage, Stage::Idle);
    let req = match (stage, resp) {
        (Stage::LoadingTree(work), Response::Got(found)) => {
            let mut tree: Tree = found.map(|b| decode(&b)).unwrap_or_default();
            work.taken.iter().flat_map(|t| &t.msgs).for_each(|(k, v)| {
                if tree.get(k).is_none_or(|old| v.ts > old.ts) {
                    tree.insert(k.clone(), v.clone());
                }
            });
            // A leaf keeps no tombstones.
            tree.retain(|_, v| !matches!(v.msg, Op::Delete));
            let seg = SegmentId {
                epoch: w.epoch,
                seq: w.next_segment,
            };
            w.next_segment += 1;
            w.stage = Stage::PuttingSegment(work, seg);
            Request::Put(Path::Segment(seg), encode(&tree))
        }
        (Stage::PuttingSegment(work, seg), Response::Done) => {
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
                    epoch: w.epoch,
                    seq: w.next_segment,
                },
            };
            let next =
                checkpoint::next_manifest(&w.manifest, w.epoch, work.taken.as_ref(), done, clock);
            let req = Request::PutIfAbsent(Path::Manifest(w.manifest_id.next().0), encode(&next));
            w.stage = Stage::Publishing(work, Box::new(next));
            req
        }
        (Stage::Publishing(_, next), Response::Created) => {
            w.manifest_id = w.manifest_id.next();
            w.tail.prune(next.wal_applied, clock);
            w.manifest = Arc::new(*next);
            p.arm(Timer::Checkpoint);
            return;
        }
        // A newer writer published: this one is fenced.
        (Stage::Publishing(..), Response::Exists(_)) => {
            p.phase = Phase::Stopped;
            return;
        }
        // A failed request ends the attempt; the next one starts over.
        (_, _) => {
            p.arm(Timer::Checkpoint);
            return;
        }
    };
    p.send(Purpose::Checkpoint, req);
}

// ---------------------------------------------------------------------------
// Properties

/// What a fresh reader sees for every key: the newest manifest's tree plus
/// every log batch past its applied slot, up to the first empty one.
pub fn visible(objects: &BTreeMap<Path, Vec<u8>>) -> Tree {
    let Some((_, m)) = manifest_id(objects) else {
        return Tree::new();
    };
    let tree: Tree = m
        .tree
        .and_then(|r| objects.get(&Path::Segment(r.ptr.seg)))
        .map(|b| decode(b))
        .unwrap_or_default();
    std::iter::successors(Some(m.wal_applied.next()), |s| Some(s.next()))
        .map_while(|s| objects.get(&Path::Log(s)).map(|b| decode::<WalRecord>(b)))
        .filter_map(|r| match r {
            WalRecord::Batch(b) => Some(b),
            WalRecord::Fence(_) => None,
        })
        .flat_map(|b| b.ops)
        .fold(tree, |mut tree, (k, v)| {
            if tree.get(&k).is_none_or(|old| v.ts > old.ts) {
                tree.insert(k, v);
            }
            tree
        })
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

fn stored_batches(store: &BTreeMap<Path, Vec<u8>>) -> impl Iterator<Item = WalBatch> + '_ {
    store
        .iter()
        .filter(|(p, _)| matches!(p, Path::Log(_)))
        .filter_map(|(_, b)| match decode::<WalRecord>(b) {
            WalRecord::Batch(batch) => Some(batch),
            WalRecord::Fence(_) => None,
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
/// the process handles the response, as one step), fire one of its timers,
/// or collect the log. Handling a response touches only the process's own
/// state, so it commutes with other processes' steps: fusing the two loses
/// no behaviour (partial-order reduction) and cuts the state space by
/// orders of magnitude.
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
    /// Deletes the log slots the newest manifest lets go, as if past the
    /// retention.
    Gc,
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

/// The log slots GC would delete now.
fn collectable(cfg: &Config, store: &BTreeMap<Path, Vec<u8>>) -> Vec<WalSeq> {
    let Some((_, m)) = manifest_id(store) else {
        return Vec::new();
    };
    let stored: Vec<WalSeq> = store
        .keys()
        .filter_map(|p| match p {
            Path::Log(s) => Some(*s),
            _ => None,
        })
        .collect();
    match cfg.gc {
        LogGc::Off => Vec::new(),
        LogGc::KeepFences => deletable_wals(&stored, &m).collect(),
        LogGc::DeleteFences => stored.into_iter().filter(|s| *s <= m.wal_applied).collect(),
    }
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

/// Every batch durable in the log stays visible or superseded, acknowledged
/// or not: a manifest must not count a slot as applied that its tree lacks.
pub fn durable_writes_stay_visible(_: &Checked, s: &Sys) -> bool {
    let batches: Vec<WalBatch> = stored_batches(&s.store).collect();
    let durable = newest(
        batches
            .iter()
            .flat_map(|b| b.ops.iter().map(|(k, v)| (k, v))),
    );
    covers(&visible(&s.store), &durable)
}

/// Log order is timestamp order, across writers: a checkpoint applying a
/// prefix never drops a tombstone that a version left behind is older than.
pub fn log_order_is_timestamp_order(_: &Checked, s: &Sys) -> bool {
    let stamps: Vec<Ts> = stored_batches(&s.store)
        .flat_map(|b| b.ops.into_iter().map(|(_, v)| v.ts))
        .collect();
    stamps.windows(2).all(|w| w[0] < w[1])
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

/// A writer got fenced after acknowledging writes.
fn a_writer_gets_fenced(_: &Checked, s: &Sys) -> bool {
    s.procs
        .iter()
        .any(|p| p.phase == Phase::Stopped && !p.acked.is_empty())
}

impl Model for Checked {
    type State = Sys;
    type Action = Action;

    fn init_states(&self) -> Vec<Sys> {
        vec![Sys {
            store: BTreeMap::new(),
            procs: (0..self.cfg.scripts.len()).map(|_| start()).collect(),
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
        if !collectable(&self.cfg, &s.store).is_empty() {
            actions.push(Action::Gc);
        }
    }

    fn next_state(&self, s: &Sys, action: Action) -> Option<Sys> {
        let mut s = s.clone();
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
                    on_response(p, &self.cfg, purpose, resp);
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
                p.timers.retain(|t| *t != timer);
                fire(p, &self.cfg, proc, &timer);
            }
            Action::Gc => collectable(&self.cfg, &s.store)
                .into_iter()
                .for_each(|seq| {
                    s.store.remove(&Path::Log(seq));
                }),
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
            Property::always("log order is timestamp order", log_order_is_timestamp_order),
            Property::sometimes("a tombstone gets dropped", a_tombstone_gets_dropped),
            Property::sometimes("a writer gets fenced", a_writer_gets_fenced),
        ]
    }
}

/// Replaying hand-picked traces: each step must be enabled.
impl Checked {
    pub fn initial(&self) -> Sys {
        self.init_states().remove(0)
    }

    fn step(&self, s: Sys, action: Action) -> Sys {
        let mut enabled = Vec::new();
        self.actions(&s, &mut enabled);
        assert!(enabled.contains(&action), "not enabled: {action:?}");
        self.next_state(&s, action).expect("a step")
    }

    /// Fires `timer` on `proc`; panics if it isn't enabled.
    pub fn fire(&self, s: Sys, proc: usize, timer: Timer) -> Sys {
        self.step(s, Action::Fire { proc, timer })
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

    /// Collects the log; panics if there's nothing to collect.
    pub fn gc(&self, s: Sys) -> Sys {
        self.step(s, Action::Gc)
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

/// Small enough to search exhaustively: the first writer deletes a key and
/// writes another, the second (fencing it) puts the first key; GC runs.
pub fn small() -> Config {
    Config {
        scripts: vec![
            vec![Write::Delete("k"), Write::Put("x", "1")],
            vec![Write::Put("k", "b")],
        ],
        max_clock: 3,
        max_manifest: 4,
        place_fence: true,
        observe_last_ts: true,
        gc: LogGc::KeepFences,
        max_faults: 0,
    }
}

/// `small` with the protocol rule `rule` broken.
pub fn without(rule: Rule) -> Config {
    let cfg = small();
    match rule {
        Rule::Fence => Config {
            place_fence: false,
            ..cfg
        },
        Rule::LastTs => Config {
            observe_last_ts: false,
            ..cfg
        },
        Rule::KeepFences => Config {
            gc: LogGc::DeleteFences,
            ..cfg
        },
    }
}

/// A protocol rule a bug config breaks.
#[derive(Clone, Copy, Debug)]
pub enum Rule {
    /// A new writer fences the log.
    Fence,
    /// A new writer's clock passes the manifest's newest timestamp.
    LastTs,
    /// GC keeps fences.
    KeepFences,
}

impl Sys {
    /// Writes `proc` had acknowledged.
    pub fn acked(&self, proc: usize) -> &[(Bytes, Version)] {
        &self.procs[proc].acked
    }

    /// Whether `proc`'s writer is open.
    pub fn is_open(&self, proc: usize) -> bool {
        matches!(self.procs[proc].phase, Phase::Open(_))
    }

    /// Whether `proc`'s writer stopped (fenced or superseded).
    pub fn is_stopped(&self, proc: usize) -> bool {
        self.procs[proc].phase == Phase::Stopped
    }
}
