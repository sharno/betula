//! A writer. Any number of processes can open writers on the same database at
//! the same time; they never coordinate on the write path.
//!
//! Write path: each write gets a hybrid-clock timestamp and goes into this
//! writer's own log (`writers/{id}/wal/{seq}`, put-if-absent). Once durable it
//! is acknowledged and visible here; other processes see it after their next
//! poll.
//!
//! One process at a time also holds the applier role. It folds every writer's
//! unapplied log entries into the tree (copy-on-write, one segment object per
//! checkpoint) and publishes the next manifest with put-if-absent, which also
//! fences an older applier. Any writer takes over the role when entries sit
//! unapplied for too long. Losing the role never affects writing.
//!
//! Versions merge newest-wins, so applying entries in any order is correct, and
//! a slow writer's late entries need no special handling.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::error::{Error, Result};
use crate::follow::{Follower, Scope};
use crate::gc::{self, GcConfig, GcStats};
use crate::hlc::{Hlc, Ts, WriterId};
use crate::location;
use crate::logs::{Logs, Region, RegionStores};
use crate::manifest::{self, Manifest, Publish};
use crate::nodes::{CacheStats, Nodes};
use crate::retry::retry;
use crate::segments::{self, SegmentKind, SegmentTable};
use crate::settings::{ApplierRole, Candidacy, Follow, Gc, Settings};
use crate::snapshot::DbIterator;
use crate::store::{RequestStats, Store};
use crate::tree::{self, TreeShape};
use crate::types::{Epoch, KeyRange, ManifestId, Msg, SegmentId, TreeRoot, Version, entry_bytes};
use crate::wal::{self, Lease, WalBatch, WalWrite};
use std::ops::RangeBounds;

const CMD_QUEUE: usize = 1024;

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Return once the batch is in a durable log object (and visible here).
    #[default]
    AwaitDurable,
    /// Return once the batch is queued. It becomes durable and visible with
    /// the next log flush.
    Queued,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOptions {
    pub durability: Durability,
}

/// How current a read must be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Freshness {
    /// What this process has seen: its own writes immediately, other
    /// writers' within a poll interval.
    #[default]
    Local,
    /// First pull every writer's durable entries and the newest manifest
    /// (a few GETs), so the read sees every write acknowledged before it
    /// started.
    Latest,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReadOptions {
    pub freshness: Freshness,
}

/// Ops applied atomically at one timestamp. A later op on the same key in the
/// same batch replaces an earlier one.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    ops: Vec<(Bytes, Msg)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copies the key and value.
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.put_bytes(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        )
    }

    /// Takes owned buffers without copying.
    pub fn put_bytes(&mut self, key: Bytes, value: Bytes) -> &mut Self {
        self.ops.push((key, Msg::Put(value)));
        self
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        self.ops
            .push((Bytes::copy_from_slice(key.as_ref()), Msg::Delete));
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Metrics {
    pub writer: WriterId,
    pub is_applier: bool,
    pub applier_epoch: Epoch,
    pub manifest_id: ManifestId,
    /// Time since the current manifest was published.
    pub manifest_age: Duration,
    pub tree: Option<TreeRoot>,
    /// Durable writes this process knows of that the tree doesn't hold yet.
    pub unapplied_bytes: usize,
    /// How long the oldest of them has waited.
    pub unapplied_age: Option<Duration>,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
    /// Batches this writer made durable, and the log objects holding them.
    pub batches_written: u64,
    pub log_flushes: u64,
    /// Checkpoints this process published as applier.
    pub checkpoints: u64,
    pub last_checkpoint: Option<Duration>,
    pub cache: CacheStats,
    pub requests: RequestStats,
}

#[derive(Debug, Default)]
struct Counters {
    batches: AtomicU64,
    flushes: AtomicU64,
    checkpoints: AtomicU64,
    /// Duration of the last checkpoint in microseconds plus one (0: none).
    last_checkpoint_us: AtomicU64,
}

struct Applier {
    epoch: Epoch,
    next_segment: SegmentId,
}

struct Shared {
    nodes: Arc<Nodes>,
    follower: Follower,
    /// Where this writer's log goes (`cfg.log`, resolved).
    own_log: Store,
    cfg: Settings,
    clock: Arc<dyn Clock>,
    writer: WriterId,
    hlc: Hlc,
    /// Timestamps handed out but not yet durable (multiset).
    inflight: Mutex<BTreeMap<u64, usize>>,
    /// The writer must not acknowledge writes at or after this (own clock).
    lease_valid_until_ms: AtomicU64,
    applier: tokio::sync::Mutex<Option<Applier>>,
    is_applier: AtomicBool,
    /// Set once this writer can no longer write.
    fatal: OnceLock<Error>,
    shutdown: watch::Sender<bool>,
    wake_applier: Notify,
    counters: Counters,
}

impl Shared {
    fn store(&self) -> &Store {
        self.nodes.store()
    }

    fn now(&self) -> u64 {
        self.clock.now_ms()
    }

    /// What background refreshes pull. The applier always needs every log.
    fn scope(&self) -> Scope {
        match self.cfg.poll.follow {
            Follow::AllWriters => Scope::AllWriters,
            Follow::ManifestOnly if self.is_applier.load(Ordering::Acquire) => Scope::AllWriters,
            Follow::ManifestOnly => Scope::ManifestOnly,
        }
    }

    fn check(&self) -> Result<()> {
        self.fatal.get().map_or(Ok(()), |e| Err(e.clone()))
    }

    fn fail(&self, e: Error) -> Error {
        let e = self.fatal.get_or_init(|| e).clone();
        self.follower.installed.notify_waiters();
        e
    }

    /// A lower bound on every timestamp this writer may still make durable.
    /// Takes a fresh clock reading so an idle writer's floor keeps moving.
    fn floor_hlc(&self) -> u64 {
        let floor = self.hlc.now(self.now());
        self.inflight
            .lock()
            .keys()
            .next()
            .map_or(floor, |&oldest| oldest.min(floor))
    }

    async fn write_lease(&self, expires_at_ms: u64) -> Result<()> {
        let lease = Lease {
            writer: self.writer,
            log: self.cfg.log.clone(),
            candidacy: self.cfg.applier.candidacy(),
            floor_hlc: self.floor_hlc(),
            expires_at_ms,
        };
        wal::write_lease(self.store(), &lease).await
    }

    async fn renew_lease(&self) -> Result<()> {
        let ttl = self.cfg.lease.ttl.as_millis() as u64;
        let started = self.now();
        self.write_lease(started + ttl).await?;
        // Stop a quarter-ttl early to absorb clock skew with the applier.
        self.lease_valid_until_ms
            .store(started + ttl - ttl / 4, Ordering::Release);
        Ok(())
    }

    fn set_role(&self, guard: &mut Option<Applier>, role: Option<Applier>) {
        self.is_applier.store(role.is_some(), Ordering::Release);
        *guard = role;
    }

    /// Takes the applier role by publishing the next manifest with a bumped
    /// epoch. Returns whether this process now holds it.
    async fn claim(&self) -> Result<bool> {
        let mut guard = self.applier.lock().await;
        if guard.is_some() {
            return Ok(true);
        }
        self.follower
            .refresh(true, Scope::AllWriters, self.now())
            .await?;
        let store = self.store();
        let view = self.follower.view();
        let (id, current) = match view.manifest_id {
            ManifestId(0) => {
                // Losing this race is fine: the winner's manifest is used.
                manifest::publish(store, ManifestId(1), &Manifest::initial(self.now())).await?;
                match manifest::load(store, ManifestId(1)).await? {
                    Some(m) => (ManifestId(1), m),
                    None => return Ok(false),
                }
            }
            id => (id, view.manifest.as_ref().clone()),
        };
        let next = current.fenced(self.writer, self.now());
        let next_id = id.next();
        if manifest::publish(store, next_id, &next).await? == Publish::Conflict {
            return Ok(false);
        }
        delete_orphans(store, next.segment_watermark, next.applier_epoch).await?;
        let epoch = next.applier_epoch;
        self.follower.install(next_id, Arc::new(next), self.now());
        tracing::info!(writer = %self.writer, epoch = epoch.0, "took the applier role");
        self.set_role(
            &mut guard,
            Some(Applier {
                epoch,
                next_segment: SegmentId { epoch, seq: 0 },
            }),
        );
        Ok(true)
    }

    async fn maybe_claim(&self) -> Result<()> {
        if self.is_applier.load(Ordering::Acquire) {
            return Ok(());
        }
        let view = self.follower.view();
        let now = self.now();
        let waiting_for = |d: Duration| {
            let d = d.as_millis() as u64;
            view.tails.since_ms.is_some_and(|t| now >= t + d)
        };
        let steal_after = match self.cfg.applier {
            ApplierRole::Auto { steal_after } => steal_after,
            ApplierRole::Standby { after } => {
                // Whatever the reason nobody applied (no applier, stuck,
                // dead), the home region had `after` to do it.
                let idle = now >= view.manifest.created_at_ms + after.as_millis() as u64;
                if waiting_for(after) && idle && view.manifest.applier != Some(self.writer) {
                    self.claim().await?;
                }
                return Ok(());
            }
            ApplierRole::Never => return Ok(()),
        };
        let take = match view.manifest.applier {
            _ if view.manifest_id == ManifestId(0) => true,
            // Released on close: take it as soon as there is work.
            None => !view.tails.is_empty(),
            Some(w) if w == self.writer => false,
            // Alive but stuck: work waited past `steal_after` and the
            // applier published nothing meanwhile.
            Some(_)
                if waiting_for(steal_after)
                    && now >= view.manifest.created_at_ms + steal_after.as_millis() as u64 =>
            {
                true
            }
            // Work waited a couple of checkpoint intervals: take over at once
            // if the applier's lease ran out (it crashed or lost the store).
            Some(w) if waiting_for(self.cfg.checkpoint.interval * 2) => {
                wal::read_lease(self.store(), w)
                    .await?
                    .is_none_or(|l| l.expires_at_ms <= now)
            }
            Some(_) => false,
        };
        if take {
            self.claim().await?;
        }
        Ok(())
    }

    /// Gives up the applier role so another writer can take it at once.
    async fn release(&self) -> Result<()> {
        let mut guard = self.applier.lock().await;
        self.release_locked(&mut guard).await
    }

    async fn release_locked(&self, guard: &mut Option<Applier>) -> Result<()> {
        if guard.is_none() {
            return Ok(());
        }
        self.set_role(guard, None);
        let view = self.follower.view();
        let next = view.manifest.released(self.now());
        manifest::publish(self.store(), view.manifest_id.next(), &next).await?;
        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        let view = self.follower.view();
        let interval = self.cfg.checkpoint.interval.as_millis() as u64;
        view.tails.mem().bytes() >= self.cfg.checkpoint.bytes
            || view
                .tails
                .since_ms
                .is_some_and(|t| self.now() >= t + interval)
    }

    /// Lowest floor among writers with a live lease. Must be read *before*
    /// pulling log entries, and the pull must cover every writer listed here:
    /// an entry made durable before its writer's lease was written is then
    /// already visible to the pull. (Pulling only previously known writers
    /// let a new writer's older entry arrive after a newer tombstone for the
    /// same key had been dropped, resurrecting the key.)
    ///
    /// Also returns every writer with objects in storage, and whether a
    /// primary candidate other than this writer is alive.
    async fn lease_floor(&self) -> Result<LeaseScan> {
        let store = self.store();
        let now = self.now();
        let writers = store.list_writers().await?;
        self.follower.track(writers.iter().copied());
        let leases =
            futures::future::try_join_all(writers.iter().map(|w| wal::read_lease(store, *w)))
                .await?;
        let logs = self.follower.logs();
        let live: Vec<Lease> = leases
            .into_iter()
            .flatten()
            .inspect(|l| logs.learn(l))
            .filter(|l| l.expires_at_ms > now)
            .collect();
        let floor = live
            .iter()
            .map(|l| l.floor_hlc)
            .chain([self.floor_hlc()])
            .min()
            .expect("chained with our own floor");
        Ok(LeaseScan {
            floor,
            present: writers.into_iter().collect(),
            primary_alive: live
                .iter()
                .any(|l| l.candidacy == Candidacy::Primary && l.writer != self.writer),
        })
    }

    /// Folds every pulled log entry into the tree and publishes a manifest.
    async fn checkpoint(&self) -> Result<()> {
        let mut guard = self.applier.lock().await;
        let Some(app) = guard.as_mut() else {
            return Err(Error::NotApplier);
        };
        let started = Instant::now();
        let LeaseScan {
            floor,
            present,
            primary_alive,
        } = self.lease_floor().await?;
        self.follower
            .refresh(false, Scope::AllWriters, self.now())
            .await?;
        let view = self.follower.view();
        if view.manifest.applier_epoch > app.epoch {
            self.set_role(&mut guard, None);
            return Err(Error::NotApplier);
        }
        let victims = segments::pick_victims(&view.manifest.segments, &self.cfg.cleaner);
        if view.tails.is_empty() && victims.is_empty() {
            return Ok(());
        }
        // A tombstone can go once no writer can still produce an older
        // version of its key. (Older versions already in the tree are carried
        // down to the leaf whenever the leaf is rewritten.)
        let drop_below = Ts {
            hlc: floor,
            writer: WriterId(0),
        };
        let out = tree::Outputs {
            inner: app.next_segment,
            leaves: app.next_segment.next(),
        };
        // Fresh segments per attempt: a failed attempt may still have
        // uploaded its segments, which then show up as orphans.
        app.next_segment = out.leaves.next();
        let msgs = view.tails.mem().to_sorted_vec();
        let msg_count = msgs.len();
        let msg_bytes = view.tails.mem().bytes();
        let ck = tree::Checkpoint {
            out,
            victims: &victims,
            drop_tombstones_below: drop_below,
            compression: self.cfg.compression,
        };
        let plan = tree::apply(&self.nodes, &self.cfg.tree, ck, view.manifest.tree, msgs).await?;
        futures::future::try_join_all(
            plan.segments
                .iter()
                .map(|segment| retry("segment upload", || self.nodes.upload(segment))),
        )
        .await?;
        let new: Vec<(SegmentId, u64, SegmentKind)> = plan
            .segments
            .iter()
            .map(|s| {
                let kind = if s.id == out.inner {
                    SegmentKind::Inner
                } else {
                    SegmentKind::Leaf
                };
                (s.id, s.data.len() as u64, kind)
            })
            .collect();
        let (table, dead) = segments::account(&view.manifest.segments, &new, &plan.freed);
        let next = Manifest {
            applier_epoch: app.epoch,
            applier: Some(self.writer),
            tree: plan.root,
            // Writers GC forgot (no lease, no logs) can never write again.
            frontier: view
                .tails
                .frontier(&view.manifest.frontier)
                .into_iter()
                .filter(|(w, _)| present.contains(w))
                .collect(),
            segment_watermark: app.next_segment,
            segments: table,
            dead,
            created_at_ms: self.now(),
        };
        let next_id = view.manifest_id.next();
        match retry("manifest publish", || {
            manifest::publish(self.store(), next_id, &next)
        })
        .await?
        {
            Publish::Published => {
                let bytes_of = |id: SegmentId| {
                    new.iter()
                        .find(|(s, _, _)| *s == id)
                        .map_or(0, |(_, b, _)| *b)
                };
                tracing::debug!(
                    manifest = next_id.0,
                    msgs = msg_count,
                    msg_bytes,
                    inner_bytes = bytes_of(out.inner),
                    leaf_bytes = bytes_of(out.leaves),
                    moved_bytes = victims.iter().map(|v| view.manifest.segments.get(v).map_or(0, |u| u.live)).sum::<u64>(),
                    freed = plan.freed.len(),
                    victims = victims.len(),
                    elapsed = ?started.elapsed(),
                    "checkpoint published"
                );
                self.nodes.evict(&plan.freed);
                self.follower.install(next_id, Arc::new(next), self.now());
                let c = &self.counters;
                c.checkpoints.fetch_add(1, Ordering::Relaxed);
                let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX - 1);
                c.last_checkpoint_us.store(us + 1, Ordering::Relaxed);
                if primary_alive && matches!(self.cfg.applier, ApplierRole::Standby { .. }) {
                    tracing::info!(writer = %self.writer, "handing the applier role back");
                    self.release_locked(&mut guard).await?;
                }
                Ok(())
            }
            Publish::Conflict => {
                tracing::info!(writer = %self.writer, "lost the applier role");
                self.set_role(&mut guard, None);
                Err(Error::NotApplier)
            }
        }
    }
}

struct LeaseScan {
    floor: u64,
    present: BTreeSet<WriterId>,
    primary_alive: bool,
}

pub struct Db {
    shared: Arc<Shared>,
    cmd: Option<mpsc::Sender<WalCmd>>,
    tasks: Vec<JoinHandle<()>>,
}

fn random_writer_id(now_ms: u64) -> WriterId {
    // RandomState is seeded randomly per process; mixing in the time and pid
    // keeps ids distinct across processes and restarts.
    let seed = std::collections::hash_map::RandomState::new();
    WriterId(seed.hash_one((now_ms, std::process::id())))
}

/// Configures and opens a writer. See [`Db::builder`].
pub struct DbBuilder {
    store: Arc<dyn ObjectStore>,
    path: Path,
    settings: Settings,
    regions: RegionStores,
    clock: Arc<dyn Clock>,
}

impl DbBuilder {
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Replaces the wall clock (for tests and simulations).
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Adds a store holding writers' logs, overriding a URL for the same
    /// region in settings. See [`Settings::regions`].
    pub fn with_region(
        mut self,
        region: Region,
        store: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
    ) -> Self {
        self.regions.insert(region, store, path.into());
        self
    }

    pub async fn build(self) -> Result<Db> {
        let mut regions = RegionStores::from_urls(&self.settings.regions)?;
        regions.extend(self.regions);
        Db::start(self.store, self.path, self.settings, regions, self.clock).await
    }
}

impl Db {
    /// Opens a writer at a URL such as `s3://bucket/path`, `gs://`, `az://`,
    /// `file:///tmp/db` or `memory:///`. Credentials and endpoints come from
    /// the usual environment variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`);
    /// settings from [`Settings::load`].
    pub async fn open(url: &str) -> Result<Self> {
        let (store, path) = location::parse(url)?;
        Self::builder(path, store)
            .with_settings(Settings::load()?)
            .build()
            .await
    }

    /// A writer on `store` under `path`, with default settings until changed.
    pub fn builder(path: impl Into<Path>, store: Arc<dyn ObjectStore>) -> DbBuilder {
        DbBuilder {
            store,
            path: path.into(),
            settings: Settings::default(),
            regions: RegionStores::default(),
            clock: Arc::new(SystemClock),
        }
    }

    async fn start(
        os: Arc<dyn ObjectStore>,
        path: Path,
        cfg: Settings,
        regions: RegionStores,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let home = Store::new(os, path);
        let logs = Arc::new(Logs::new(home.clone(), regions));
        let own_log = logs.at(&cfg.log)?.clone();
        let nodes = Arc::new(Nodes::new(home, cfg.cache));
        let writer = random_writer_id(clock.now_ms());
        let follower =
            Follower::open(nodes.clone(), logs, cfg.warm, cfg.rewarm, Some(writer)).await?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            nodes,
            follower,
            own_log,
            writer,
            hlc: Hlc::default(),
            inflight: Mutex::new(BTreeMap::new()),
            lease_valid_until_ms: AtomicU64::new(0),
            applier: tokio::sync::Mutex::new(None),
            is_applier: AtomicBool::new(false),
            fatal: OnceLock::new(),
            shutdown,
            wake_applier: Notify::new(),
            counters: Counters::default(),
            clock,
            cfg,
        });
        shared.renew_lease().await?;
        shared
            .follower
            .refresh(true, shared.scope(), shared.now())
            .await?;
        shared.maybe_claim().await?;

        let (cmd, rx) = mpsc::channel(CMD_QUEUE);
        let wal_task = WalTask {
            shared: shared.clone(),
            next_seq: 1,
            ops: Vec::new(),
            stamps: Vec::new(),
            waiters: Vec::new(),
            bytes: 0,
        };
        let mut tasks = vec![
            tokio::spawn(wal_task.run(rx)),
            tokio::spawn(lease_loop(shared.clone(), shutdown_rx.clone())),
            tokio::spawn(follow_loop(shared.clone(), shutdown_rx.clone())),
            tokio::spawn(applier_loop(shared.clone(), shutdown_rx.clone())),
        ];
        if let Gc::On(gc_cfg) = shared.cfg.gc {
            tasks.push(tokio::spawn(gc_loop(shared.clone(), gc_cfg, shutdown_rx)));
        }
        Ok(Self {
            shared,
            cmd: Some(cmd),
            tasks,
        })
    }

    pub fn writer_id(&self) -> WriterId {
        self.shared.writer
    }

    /// Writes one key, copying it and the value; returns once durable.
    pub async fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(batch).await
    }

    /// Writes one key without copying; returns once durable.
    pub async fn put_bytes(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_bytes(key, value);
        self.write(batch).await
    }

    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch).await
    }

    /// Writes the batch atomically; returns once durable.
    pub async fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_with(batch, WriteOptions::default()).await
    }

    pub async fn write_with(&self, batch: WriteBatch, opts: WriteOptions) -> Result<()> {
        let durability = opts.durability;
        self.shared.check()?;
        if batch.ops.is_empty() {
            return Ok(());
        }
        let hlc = self.shared.hlc.now(self.shared.now());
        *self.shared.inflight.lock().entry(hlc).or_default() += 1;
        let ts = Ts {
            hlc,
            writer: self.shared.writer,
        };
        // One timestamp per batch, so collapse repeated keys: last op wins.
        let ops: Vec<(Bytes, Version)> = batch
            .ops
            .into_iter()
            .collect::<BTreeMap<Bytes, Msg>>()
            .into_iter()
            .map(|(k, msg)| (k, Version { ts, msg }))
            .collect();
        let (done, wait) = match durability {
            Durability::AwaitDurable => {
                let (tx, rx) = oneshot::channel();
                (Some(tx), Some(rx))
            }
            Durability::Queued => (None, None),
        };
        self.send(WalCmd::Write { hlc, ops, done }).await?;
        match wait {
            Some(rx) => rx.await.map_err(|_| Error::Closed)?,
            None => Ok(()),
        }
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.get_with(key, ReadOptions::default()).await
    }

    pub async fn get_with(
        &self,
        key: impl AsRef<[u8]>,
        opts: ReadOptions,
    ) -> Result<Option<Bytes>> {
        if opts.freshness == Freshness::Latest {
            self.refresh().await?;
        }
        let version = self
            .shared
            .follower
            .snapshot()
            .get(&self.shared.nodes, key.as_ref())
            .await?;
        // Later writes from this process must order after what it has seen.
        if let Some(v) = &version {
            self.shared.hlc.observe(v.ts.hlc);
        }
        Ok(version.and_then(|v| v.value().cloned()))
    }

    /// Live pairs in `range`, in key order, from a consistent snapshot.
    /// Accepts any range of byte-like keys: `"a".."m"`, `key..`, `..=key`.
    pub fn scan<K: AsRef<[u8]>>(&self, range: impl RangeBounds<K>) -> DbIterator {
        self.shared
            .follower
            .snapshot()
            .scan(self.shared.nodes.clone(), KeyRange::from_bounds(range))
    }

    pub async fn scan_with<K: AsRef<[u8]>>(
        &self,
        range: impl RangeBounds<K>,
        opts: ReadOptions,
    ) -> Result<DbIterator> {
        if opts.freshness == Freshness::Latest {
            self.refresh().await?;
        }
        Ok(self.scan(range))
    }

    /// `Ok` while this writer can write; otherwise the error that stopped it
    /// (for example a lost lease). Background failures surface here.
    pub fn status(&self) -> Result<()> {
        self.shared.check()
    }

    /// Pulls other writers' entries and the latest manifest now.
    pub async fn refresh(&self) -> Result<()> {
        self.shared
            .follower
            .refresh(true, Scope::AllWriters, self.shared.now())
            .await
            .map(|_| ())
    }

    /// Makes every write sent so far durable.
    pub async fn flush_wal(&self) -> Result<()> {
        let (done, rx) = oneshot::channel();
        self.send(WalCmd::Flush { done }).await?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Applier only: folds every durable entry from every writer into the
    /// tree now.
    pub async fn checkpoint(&self) -> Result<()> {
        self.flush_wal().await?;
        self.shared.checkpoint().await
    }

    /// Takes the applier role from whoever holds it. Returns whether this
    /// process now holds it.
    pub async fn claim_applier(&self) -> Result<bool> {
        self.shared.claim().await
    }

    pub fn is_applier(&self) -> bool {
        self.shared.is_applier.load(Ordering::Acquire)
    }

    pub async fn run_gc(&self, retention: Duration) -> Result<GcStats> {
        gc::run(
            self.shared.follower.logs(),
            self.shared.now(),
            retention,
            gc::Speed::Full,
        )
        .await
    }

    /// Walks the current tree, loading every node.
    pub async fn tree_shape(&self) -> Result<TreeShape> {
        let root = self.shared.follower.view().manifest.tree;
        tree::shape(&self.shared.nodes, root).await
    }

    /// Live and total bytes of every segment the current tree references.
    pub fn segment_table(&self) -> SegmentTable {
        self.shared.follower.view().manifest.segments.clone()
    }

    pub fn metrics(&self) -> Metrics {
        let v = self.shared.follower.view();
        let now = self.shared.now();
        let ms = |t: u64| Duration::from_millis(now.saturating_sub(t));
        let c = &self.shared.counters;
        Metrics {
            writer: self.shared.writer,
            is_applier: self.is_applier(),
            applier_epoch: v.manifest.applier_epoch,
            manifest_id: v.manifest_id,
            manifest_age: ms(v.manifest.created_at_ms),
            tree: v.manifest.tree,
            unapplied_bytes: v.tails.mem().bytes(),
            unapplied_age: v.tails.since_ms.map(ms),
            segments: v.manifest.segments.len(),
            segment_bytes: v.manifest.segments.values().map(|u| u.total).sum(),
            live_bytes: v.manifest.segments.values().map(|u| u.live).sum(),
            batches_written: c.batches.load(Ordering::Relaxed),
            log_flushes: c.flushes.load(Ordering::Relaxed),
            checkpoints: c.checkpoints.load(Ordering::Relaxed),
            last_checkpoint: c
                .last_checkpoint_us
                .load(Ordering::Relaxed)
                .checked_sub(1)
                .map(Duration::from_micros),
            cache: self.shared.nodes.cache_stats(),
            requests: self.shared.store().requests(),
        }
    }

    /// Flushes pending writes, applies them if this process is the applier,
    /// marks the lease expired, and stops background work. Entries not yet
    /// applied are applied later by whoever holds the role.
    pub async fn close(mut self) -> Result<()> {
        // Dropping the sender makes the log task flush and exit.
        self.cmd.take();
        let mut tasks = std::mem::take(&mut self.tasks).into_iter();
        if let Some(wal) = tasks.next() {
            let _ = wal.await;
        }
        self.shared.shutdown.send_replace(true);
        futures::future::join_all(tasks).await;
        // A free (or dead applier's) role: take it so our writes get applied
        // now rather than whenever another writer next runs.
        if let Err(e) = self.shared.maybe_claim().await {
            tracing::debug!(%e, "claim on close failed");
        }
        if self.is_applier() {
            match self.shared.checkpoint().await {
                Ok(()) => {
                    if let Err(e) = self.shared.release().await {
                        tracing::warn!(%e, "releasing the applier role failed");
                    }
                }
                Err(Error::NotApplier) => {}
                Err(e) => tracing::warn!(%e, "final checkpoint failed"),
            }
        }
        // Expire the lease so tombstone collection doesn't wait for us.
        let _ = self.shared.write_lease(self.shared.now()).await;
        match self.shared.fatal.get() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    async fn send(&self, cmd: WalCmd) -> Result<()> {
        let tx = self.cmd.as_ref().ok_or(Error::Closed)?;
        tx.send(cmd).await.map_err(|_| Error::Closed)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if self.cmd.is_some() {
            tracing::warn!(
                writer = %self.shared.writer,
                "Db dropped without close(): queued (not yet durable) writes may be lost; \
                 durable ones are applied later by whoever holds the applier role"
            );
        }
        self.shared.shutdown.send_replace(true);
    }
}

/// Segments at or above the published watermark from earlier epochs were
/// uploaded by an applier that died (or lost the role) before publishing.
async fn delete_orphans(store: &Store, watermark: SegmentId, epoch: Epoch) -> Result<()> {
    let orphans: Vec<_> = store
        .list_segments_from(watermark)
        .await?
        .into_iter()
        .filter(|id| id.epoch < epoch)
        .map(|id| store.segment_path(id))
        .collect();
    if !orphans.is_empty() {
        tracing::info!(count = orphans.len(), "deleting orphaned segments");
    }
    store.delete_all(orphans).await
}

// ---------------------------------------------------------------------------
// Log

enum WalCmd {
    Write {
        hlc: u64,
        ops: Vec<(Bytes, Version)>,
        done: Option<oneshot::Sender<Result<()>>>,
    },
    Flush {
        done: oneshot::Sender<Result<()>>,
    },
}

struct WalTask {
    shared: Arc<Shared>,
    next_seq: u64,
    ops: Vec<(Bytes, Version)>,
    /// Timestamps of the pending ops, released from `inflight` once durable.
    stamps: Vec<u64>,
    waiters: Vec<oneshot::Sender<Result<()>>>,
    bytes: usize,
}

impl WalTask {
    async fn run(mut self, mut rx: mpsc::Receiver<WalCmd>) {
        let mut tick = tokio::time::interval(self.shared.cfg.wal.flush_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(WalCmd::Write { hlc, ops, done }) => {
                        self.bytes += ops.iter().map(|(k, v)| entry_bytes(k, v.msg.encoded_len())).sum::<usize>();
                        self.ops.extend(ops);
                        self.stamps.push(hlc);
                        self.waiters.extend(done);
                        if self.bytes >= self.shared.cfg.wal.max_bytes {
                            let _ = self.flush().await;
                        }
                    }
                    Some(WalCmd::Flush { done }) => {
                        let _ = done.send(self.flush().await);
                    }
                    None => {
                        let _ = self.flush().await;
                        return;
                    }
                },
                _ = tick.tick() => {
                    if !self.ops.is_empty() {
                        let _ = self.flush().await;
                    }
                }
            }
        }
    }

    async fn flush(&mut self) -> Result<()> {
        let ops = std::mem::take(&mut self.ops);
        let stamps = std::mem::take(&mut self.stamps);
        let waiters = std::mem::take(&mut self.waiters);
        self.bytes = 0;
        let result = self.persist(ops, stamps.len() as u64).await;
        {
            let mut inflight = self.shared.inflight.lock();
            stamps.iter().for_each(|t| {
                if let Some(n) = inflight.get_mut(t) {
                    *n -= 1;
                    if *n == 0 {
                        inflight.remove(t);
                    }
                }
            });
        }
        waiters.into_iter().for_each(|w| {
            let _ = w.send(result.clone());
        });
        result
    }

    /// Writes `ops` (from `batches` write calls) as one log object.
    async fn persist(&mut self, ops: Vec<(Bytes, Version)>, batches: u64) -> Result<()> {
        let shared = &self.shared;
        shared.check()?;
        if ops.is_empty() {
            return Ok(());
        }
        // Backpressure: wait while the applier is far behind.
        loop {
            let installed = shared.follower.installed.notified();
            shared.check()?;
            if shared.follower.tails_bytes() < shared.cfg.checkpoint.max_unapplied_bytes {
                break;
            }
            installed.await;
        }
        if shared.now() >= shared.lease_valid_until_ms.load(Ordering::Acquire) {
            return Err(shared.fail(Error::LeaseExpired));
        }
        let batch = WalBatch {
            writer: shared.writer,
            ops,
        };
        let seq = self.next_seq;
        let compression = shared.cfg.compression;
        match retry("wal write", || {
            wal::write(&shared.own_log, seq, &batch, compression)
        })
        .await
        {
            Ok(WalWrite::Written) => {}
            Ok(WalWrite::Taken) => return Err(shared.fail(Error::WriterIdTaken)),
            // The outcome is unknown, so the next batch can't safely reuse
            // this sequence number: stop writing.
            Err(e) => return Err(shared.fail(e)),
        }
        self.next_seq += 1;
        let c = &shared.counters;
        c.flushes.fetch_add(1, Ordering::Relaxed);
        c.batches.fetch_add(batches, Ordering::Relaxed);
        shared.follower.add_own(seq, Arc::new(batch), shared.now());
        shared.wake_applier.notify_one();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Background loops

async fn every(period: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(period) => !*shutdown.borrow(),
        _ = shutdown.changed() => false,
    }
}

async fn lease_loop(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    while every(shared.cfg.lease.interval, &mut shutdown).await {
        if let Err(e) = shared.renew_lease().await {
            tracing::warn!(%e, "lease renewal failed");
        }
    }
}

async fn follow_loop(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let mut last_discovery = Instant::now();
    let mut period = shared.cfg.poll.interval;
    while every(period, &mut shutdown).await {
        let discover = last_discovery.elapsed() >= shared.cfg.poll.discovery_interval;
        if discover {
            last_discovery = Instant::now();
        }
        match shared
            .follower
            .refresh(discover, shared.scope(), shared.now())
            .await
        {
            // Poll fast while things arrive, back off while quiet.
            Ok(true) => period = shared.cfg.poll.interval,
            Ok(false) => period = (period * 2).min(shared.cfg.poll.max_interval),
            Err(e) => {
                tracing::warn!(%e, "refresh failed");
                continue;
            }
        }
        if let Err(e) = shared.maybe_claim().await {
            tracing::warn!(%e, "applier claim failed");
        }
        shared.wake_applier.notify_one();
    }
}

async fn applier_loop(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = shared.wake_applier.notified() => {}
            _ = tokio::time::sleep(shared.cfg.poll.interval) => {}
            _ = shutdown.changed() => return,
        }
        if *shutdown.borrow() {
            return;
        }
        if !shared.is_applier.load(Ordering::Acquire) || !shared.should_checkpoint() {
            continue;
        }
        match shared.checkpoint().await {
            Ok(()) | Err(Error::NotApplier) => {}
            Err(e) => {
                tracing::warn!(%e, "checkpoint failed, retrying");
                let _ = every(Duration::from_secs(1), &mut shutdown).await;
            }
        }
    }
}

async fn gc_loop(shared: Arc<Shared>, cfg: GcConfig, mut shutdown: watch::Receiver<bool>) {
    while every(cfg.interval, &mut shutdown).await {
        if !shared.is_applier.load(Ordering::Acquire) {
            continue;
        }
        let speed = gc::Speed::Spread {
            over: cfg.interval,
            max_per_sec: cfg.max_deletes_per_sec,
        };
        match gc::run(shared.follower.logs(), shared.now(), cfg.retention, speed).await {
            Ok(stats) => tracing::debug!(?stats, "gc"),
            Err(e) => tracing::warn!(%e, "gc failed"),
        }
    }
}
