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
//! a slow writer's late entries need no special handling. A writer presumed
//! dead has its log sealed first (see `wal`), so none can arrive after the
//! applier stopped waiting for it.

use std::collections::BTreeMap;
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
use crate::protocol::log::{self, Gate, Inflight, LogWriter, PutOutcome, Stopped};
use crate::protocol::{applier, checkpoint, tombstones};
use crate::segments::{self, SegmentKind, SegmentTable};
use crate::settings::{Follow, Gc, Settings};
use crate::snapshot::DbIterator;
use crate::store::{RequestStats, Store};
use crate::tree::{self, TreeShape};
use crate::types::{Epoch, KeyRange, ManifestId, Msg, SegmentId, TreeRoot, Version};
use crate::wal::{self, Lease, WalWrite};
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
    inflight: Mutex<Inflight>,
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

    /// Moves the clock past the newest manifest's tombstone bound. Required
    /// once after the lease is written and before the first write; repeated
    /// on every poll to keep the clock close to other writers'.
    fn observe_manifest(&self) {
        self.hlc.observe(self.follower.manifest().tombstone_bound);
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
        let now = self.hlc.now(self.now());
        self.inflight.lock().floor(now)
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
        self.write_lease(self.now() + ttl).await
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
        let situation = applier::Situation {
            me: self.writer,
            now_ms: now,
            manifest_id: view.manifest_id,
            manifest: &view.manifest,
            waiting_since_ms: view.tails.since_ms,
            checkpoint_interval: self.cfg.checkpoint.interval,
        };
        let take = match applier::claim(self.cfg.applier, &situation) {
            applier::Claim::No => false,
            applier::Claim::Yes => true,
            applier::Claim::IfLeaseExpired(w) => {
                applier::lease_expired(wal::read_lease(self.store(), w).await?.as_ref(), now)
            }
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
        applier::due(
            &self.cfg.checkpoint,
            view.tails.mem().bytes(),
            view.tails.since_ms,
            self.now(),
        )
    }

    /// Every writer's lease, and this writer's floor. Must be read *before*
    /// pulling log entries, and the pull must cover every writer listed here:
    /// an entry made durable before its writer's lease was written is then
    /// already visible to the pull. (Pulling only previously known writers
    /// let a new writer's older entry arrive after a newer tombstone for the
    /// same key had been dropped, resurrecting the key.)
    async fn scan_leases(&self) -> Result<checkpoint::Scanned> {
        let store = self.store();
        let writers = store.list_writers().await?;
        self.follower.track(writers.iter().copied());
        let leases: Vec<Lease> =
            futures::future::try_join_all(writers.iter().map(|w| wal::read_lease(store, *w)))
                .await?
                .into_iter()
                .flatten()
                .collect();
        let logs = self.follower.logs();
        leases.iter().for_each(|l| logs.learn(l));
        Ok(checkpoint::Scanned {
            own_floor: self.floor_hlc(),
            leases,
            present: writers.into_iter().collect(),
        })
    }

    /// Seals the logs of other writers whose lease ran out, pulling their
    /// last entries. A log that can't be reached stays unsealed, so its
    /// writer keeps holding tombstones back.
    async fn seal_writers(&self, writers: &[WriterId]) -> Result<()> {
        let now = self.now();
        let sealed = futures::future::join_all(writers.iter().map(|&w| async move {
            let r = self.follower.seal(w, now).await;
            if r.is_ok() {
                tracing::info!(writer = %w, "sealed the log of an expired writer");
            }
            r
        }))
        .await;
        sealed.into_iter().try_for_each(|r| match r {
            Err(e) if e.is_retryable() => {
                tracing::warn!(%e, "sealing an expired writer's log");
                Ok(())
            }
            other => other,
        })
    }

    /// Folds every pulled log entry into the tree and publishes a manifest.
    async fn checkpoint(&self) -> Result<()> {
        let mut guard = self.applier.lock().await;
        let Some(app) = guard.as_mut() else {
            return Err(Error::NotApplier);
        };
        let started = Instant::now();
        // The stages and why they're ordered so: `protocol::checkpoint`.
        let scanned = self.scan_leases().await?;
        let leased = started.elapsed();
        let pull = match self
            .follower
            .refresh(false, Scope::AllWriters, self.now())
            .await?
            .unreachable
            .is_empty()
        {
            true => tombstones::Pull::Complete,
            false => tombstones::Pull::Partial,
        };
        let sealing = scanned.pulled(&self.follower.view(), self.writer, self.now(), pull);
        self.seal_writers(&sealing.to_seal).await?;
        let pulled = started.elapsed();
        let misses_before = self.nodes.cache_stats().misses;
        let view = self.follower.view();
        if view.manifest.applier_epoch > app.epoch {
            self.set_role(&mut guard, None);
            return Err(Error::NotApplier);
        }
        let who = checkpoint::Applier {
            me: self.writer,
            epoch: app.epoch,
            role: self.cfg.applier,
            cleaner: &self.cfg.cleaner,
        };
        let mut applying = match sealing.sealed(&view, who, self.now()) {
            checkpoint::Plan::Nothing => return Ok(()),
            checkpoint::Plan::Apply(applying) => applying,
        };
        let out = tree::Outputs {
            inner: app.next_segment,
            leaves: app.next_segment.next(),
        };
        // Fresh segments per attempt: a failed attempt may still have
        // uploaded its segments, which then show up as orphans.
        app.next_segment = out.leaves.next();
        let msgs = std::mem::take(&mut applying.msgs);
        let msg_count = msgs.len();
        let msg_bytes = view.tails.mem().bytes();
        let victims = applying.victims.clone();
        let ck = tree::Checkpoint {
            out,
            victims: &victims,
            drop_tombstones_below: applying.drop_below,
            compression: self.cfg.compression,
        };
        let plan = tree::apply(&self.nodes, &self.cfg.tree, ck, view.manifest.tree, msgs).await?;
        let planned = started.elapsed();
        let node_loads = self.nodes.cache_stats().misses - misses_before;
        futures::future::try_join_all(
            plan.segments
                .iter()
                .map(|segment| self.nodes.upload(segment)),
        )
        .await?;
        let uploaded = started.elapsed();
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
        let publishing = applying.applied(
            who,
            checkpoint::Applied {
                tree: plan.root,
                segments: table,
                dead,
                segment_watermark: app.next_segment,
            },
            self.now(),
        );
        let (next, hand_back) = (publishing.manifest, publishing.hand_back);
        let next_id = view.manifest_id.next();
        match manifest::publish(self.store(), next_id, &next).await? {
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
                    node_loads,
                    run_bytes = plan.stats.run_bytes,
                    run_bytes_d0 = plan.stats.run_bytes_by_depth[0],
                    run_bytes_d1 = plan.stats.run_bytes_by_depth[1],
                    run_bytes_d2 = plan.stats.run_bytes_by_depth[2],
                    run_bytes_d3 = plan.stats.run_bytes_by_depth[3],
                    internal_bytes = plan.stats.internal_bytes,
                    relocated_run_bytes = plan.stats.relocated_run_bytes,
                    leaves_rewritten = plan.stats.leaves_rewritten,
                    leaf_msgs = plan.stats.leaf_msgs,
                    leaves_moved = plan.stats.leaves_moved,
                    leaves_underfull = plan.stats.leaves_underfull,
                    merges_free = plan.stats.merges_free,
                    merges_any = plan.stats.merges_any,
                    // Phase durations in ms: leases, log pull, tree rewrite
                    // (including node loads), segment upload, publish.
                    lease_ms = leased.as_millis(),
                    pull_ms = (pulled - leased).as_millis(),
                    plan_ms = (planned - pulled).as_millis(),
                    upload_ms = (uploaded - planned).as_millis(),
                    publish_ms = (started.elapsed() - uploaded).as_millis(),
                    elapsed = ?started.elapsed(),
                    "checkpoint published"
                );
                self.nodes.evict(&plan.freed);
                self.follower.install(next_id, Arc::new(next), self.now());
                let c = &self.counters;
                c.checkpoints.fetch_add(1, Ordering::Relaxed);
                let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX - 1);
                c.last_checkpoint_us.store(us + 1, Ordering::Relaxed);
                if hand_back {
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

/// See [`Db::audit`].
#[derive(Clone, Debug)]
pub struct Audit {
    pub shape: TreeShape,
    pub segments: SegmentTable,
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
        let home = Store::new(os, path, cfg.hedge);
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
            inflight: Mutex::new(Inflight::default()),
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
        let joined = async {
            // After the lease: see the note on `Manifest::tombstone_bound`.
            shared
                .follower
                .refresh(true, shared.scope(), shared.now())
                .await?;
            shared.observe_manifest();
            shared.maybe_claim().await
        };
        if let Err(e) = joined.await {
            // Otherwise every failed open leaves a lease that others read
            // (and pull a log for) until it expires.
            let lease = shared.store().lease_path(writer);
            if let Err(cleanup) = shared.store().delete_all(vec![lease]).await {
                tracing::warn!(%cleanup, "deleting the lease of a failed open");
            }
            return Err(e);
        }

        let (cmd, rx) = mpsc::channel(CMD_QUEUE);
        let wal_task = WalTask {
            log: LogWriter::new(writer, shared.cfg.wal.max_bytes),
            shared: shared.clone(),
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
        self.write(batch).await.map(drop)
    }

    /// Writes one key without copying; returns once durable.
    pub async fn put_bytes(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_bytes(key, value);
        self.write(batch).await.map(drop)
    }

    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch).await.map(drop)
    }

    /// Writes the batch atomically; returns once durable, with the timestamp
    /// every op in it carries (`None` for an empty batch). Across writers, the
    /// op with the newest timestamp wins.
    pub async fn write(&self, batch: WriteBatch) -> Result<Option<Ts>> {
        self.write_with(batch, WriteOptions::default()).await
    }

    pub async fn write_with(&self, batch: WriteBatch, opts: WriteOptions) -> Result<Option<Ts>> {
        let durability = opts.durability;
        self.shared.check()?;
        if batch.ops.is_empty() {
            return Ok(None);
        }
        let hlc = self.shared.hlc.now(self.shared.now());
        self.shared.inflight.lock().register(hlc);
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
        if let Some(rx) = wait {
            rx.await.map_err(|_| Error::Closed)??;
        }
        Ok(Some(ts))
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
    /// (for example a sealed log). Background failures surface here.
    pub fn status(&self) -> Result<()> {
        self.shared.check()
    }

    /// Pulls other writers' entries and the latest manifest now.
    pub async fn refresh(&self) -> Result<()> {
        self.shared
            .follower
            .refresh(true, Scope::AllWriters, self.shared.now())
            .await?
            .complete()
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

    /// The current tree's shape with its manifest's segment table, both
    /// from one manifest (separate calls can straddle a checkpoint).
    pub async fn audit(&self) -> Result<Audit> {
        let m = self.shared.follower.view().manifest;
        Ok(Audit {
            shape: tree::shape(&self.shared.nodes, m.tree).await?,
            segments: m.segments.clone(),
        })
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
            unapplied_bytes: v.tails.retained_bytes(),
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

    /// Flushes pending writes, seals this writer's log, applies it if this
    /// process is the applier, marks the lease expired, and stops background
    /// work. Entries not yet applied are applied later by whoever holds the
    /// role.
    pub async fn close(mut self) -> Result<()> {
        // Dropping the sender makes the log task write out what's pending,
        // past backpressure (`log::Event::Close`), and exit.
        self.cmd.take();
        let mut tasks = std::mem::take(&mut self.tasks).into_iter();
        if let Some(wal) = tasks.next() {
            let _ = wal.await;
        }
        self.shared.shutdown.send_replace(true);
        futures::future::join_all(tasks).await;
        // So the applier can stop holding tombstones back for us now rather
        // than once our lease expires.
        let sealed = self
            .shared
            .follower
            .seal(self.shared.writer, self.shared.now())
            .await;
        if let Err(e) = sealed {
            tracing::warn!(%e, "sealing the log on close failed");
        }
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
        // Expire the lease: if sealing failed, the applier seals for us.
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

type Waiter = oneshot::Sender<Result<()>>;

/// Drives the log state machine (`protocol::log`): feeds it writes, ticks,
/// put outcomes, and backpressure changes, and carries out its effects.
struct WalTask {
    shared: Arc<Shared>,
    log: LogWriter<Waiter>,
}

type PutFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = (u64, Result<WalWrite>)> + Send>>;

impl WalTask {
    fn gate(&self) -> Gate {
        let s = &self.shared;
        match s.follower.tails_bytes() < s.cfg.checkpoint.max_unapplied_bytes {
            true => Gate::Open,
            false => Gate::Closed,
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<WalCmd>) {
        let mut tick = tokio::time::interval(self.shared.cfg.wal.flush_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut putting: Option<PutFuture> = None;
        let mut open = true;
        // Why a put failed, for the error callers get.
        let mut failure: Option<Error> = None;
        loop {
            if !open && self.log.idle() {
                return;
            }
            let installed = self.shared.follower.installed.notified();
            let event = tokio::select! {
                cmd = rx.recv(), if open => match cmd {
                    Some(WalCmd::Write { hlc, ops, done }) => log::Event::Write { hlc, ops, waiter: done },
                    Some(WalCmd::Flush { done }) => log::Event::Flush { waiter: done },
                    None => {
                        open = false;
                        log::Event::Close
                    }
                },
                _ = tick.tick() => log::Event::Tick,
                (seq, result) = async { putting.as_mut().expect("guarded").await }, if putting.is_some() => {
                    putting = None;
                    let outcome = match result {
                        Ok(WalWrite::Written) => PutOutcome::Written,
                        Ok(WalWrite::Sealed) => PutOutcome::Sealed,
                        Ok(WalWrite::Taken) => PutOutcome::Taken,
                        Err(e) => {
                            failure = Some(e);
                            PutOutcome::Failed
                        }
                    };
                    log::Event::PutDone { seq, outcome }
                },
                _ = installed, if self.log.held_back() => log::Event::Retry,
            };
            let gate = self.gate();
            for effect in self.log.step(event, gate) {
                match effect {
                    log::Effect::Put { seq, batch } => {
                        let (store, compression) =
                            (self.shared.own_log.clone(), self.shared.cfg.compression);
                        putting = Some(Box::pin(async move {
                            (seq, wal::write(&store, seq, &batch, compression).await)
                        }));
                    }
                    log::Effect::Durable {
                        seq,
                        batch,
                        stamps,
                        writes,
                    } => {
                        let s = &self.shared;
                        s.inflight.lock().release(&stamps);
                        let c = &s.counters;
                        c.flushes.fetch_add(1, Ordering::Relaxed);
                        c.batches.fetch_add(writes, Ordering::Relaxed);
                        s.follower.add_own(seq, batch, s.now());
                        s.wake_applier.notify_one();
                    }
                    log::Effect::Ack { waiters, result } => {
                        let result = result.map_err(|why| self.stopped_error(why, &failure));
                        waiters.into_iter().for_each(|w| {
                            let _ = w.send(result.clone());
                        });
                    }
                    log::Effect::Stopped { why, stamps } => {
                        self.shared.inflight.lock().release(&stamps);
                        let e = self.stopped_error(why, &failure);
                        self.shared.fail(e);
                    }
                }
            }
        }
    }

    /// The error for a stopped log: the writer's fatal error once set.
    fn stopped_error(&self, why: Stopped, failure: &Option<Error>) -> Error {
        if let Err(e) = self.shared.check() {
            return e;
        }
        match (why, failure) {
            (Stopped::Sealed, _) => Error::LogSealed,
            (Stopped::Taken, _) => Error::WriterIdTaken,
            (Stopped::Unknown, Some(e)) => Error::LogWriteFailed(Arc::new(e.clone())),
            (Stopped::Unknown, None) => Error::Closed,
        }
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
    // A writer that can't write lets its lease lapse, so the applier seals
    // its log and stops holding tombstones back for it.
    while every(shared.cfg.lease.interval, &mut shutdown).await && shared.check().is_ok() {
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
            Ok(p) if p.changed => {
                shared.observe_manifest();
                period = shared.cfg.poll.interval
            }
            Ok(_) => period = (period * 2).min(shared.cfg.poll.max_interval),
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
