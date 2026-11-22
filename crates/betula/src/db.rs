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
use crate::follow::Follower;
use crate::gc::{self, GcConfig, GcStats};
use crate::hlc::{Hlc, Ts, WriterId};
use crate::manifest::{self, Manifest, Publish};
use crate::nodes::{CacheConfig, Nodes};
use crate::retry::retry;
use crate::segments::{self, CleanerConfig, SegmentTable};
use crate::snapshot::DbIterator;
use crate::store::Store;
use crate::tree::{self, TreeConfig, TreeShape};
use crate::types::{Epoch, KeyRange, ManifestId, Msg, SegmentId, TreeRoot, Version, entry_bytes};
use crate::wal::{self, Lease, WalBatch, WalWrite};

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

#[derive(Clone, Copy, Debug)]
pub enum ApplierRole {
    /// Take the role when the database has no applier, or when entries have
    /// sat unapplied longer than `steal_after`.
    Auto { steal_after: Duration },
    /// Only write; leave applying to other processes.
    Never,
}

#[derive(Clone, Copy, Debug)]
pub struct LeaseConfig {
    pub interval: Duration,
    /// Must comfortably exceed `interval` plus clock skew between processes.
    pub ttl: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            ttl: Duration::from_secs(20),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub tree: TreeConfig,
    pub cache: CacheConfig,
    pub wal_flush_interval: Duration,
    /// Flush the log early once this many bytes are pending.
    pub wal_max_bytes: usize,
    /// The applier checkpoints once unapplied entries reach this size...
    pub checkpoint_bytes: usize,
    /// ...or once the oldest has waited this long.
    pub checkpoint_interval: Duration,
    /// Writers stall while unapplied entries exceed this.
    pub max_unapplied_bytes: usize,
    /// How often to poll for other writers' entries and new manifests.
    pub poll_interval: Duration,
    /// How often to LIST for new writers.
    pub discovery_interval: Duration,
    pub lease: LeaseConfig,
    pub applier: ApplierRole,
    pub gc: Option<GcConfig>,
    pub cleaner: CleanerConfig,
    /// Load every internal node into cache on open.
    pub warm_inner_nodes: bool,
    /// Also load every new manifest's changed internal nodes as soon as it is
    /// seen. Otherwise they load on first use. Every writer doing this costs
    /// GETs proportional to writers × checkpoint size.
    pub rewarm_on_refresh: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            tree: TreeConfig::default(),
            cache: CacheConfig::default(),
            wal_flush_interval: Duration::from_millis(50),
            wal_max_bytes: 4 << 20,
            checkpoint_bytes: 32 << 20,
            checkpoint_interval: Duration::from_secs(2),
            max_unapplied_bytes: 512 << 20,
            poll_interval: Duration::from_millis(200),
            discovery_interval: Duration::from_secs(2),
            lease: LeaseConfig::default(),
            applier: ApplierRole::Auto {
                steal_after: Duration::from_secs(10),
            },
            gc: Some(GcConfig::default()),
            cleaner: CleanerConfig::default(),
            warm_inner_nodes: true,
            rewarm_on_refresh: false,
        }
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

    pub fn put(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.ops.push((key.into(), Msg::Put(value.into())));
        self
    }

    pub fn delete(mut self, key: impl Into<Bytes>) -> Self {
        self.ops.push((key.into(), Msg::Delete));
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DbStats {
    pub writer: WriterId,
    pub is_applier: bool,
    pub applier_epoch: Epoch,
    pub manifest_id: ManifestId,
    pub tree: Option<TreeRoot>,
    pub unapplied_bytes: usize,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
}

struct Applier {
    epoch: Epoch,
    next_segment: SegmentId,
}

struct Shared {
    nodes: Arc<Nodes>,
    follower: Follower,
    cfg: DbConfig,
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
}

impl Shared {
    fn store(&self) -> &Store {
        self.nodes.store()
    }

    fn now(&self) -> u64 {
        self.clock.now_ms()
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
        self.follower.refresh(true, self.now()).await?;
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
        let next = current.fenced(self.now());
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
        let ApplierRole::Auto { steal_after } = self.cfg.applier else {
            return Ok(());
        };
        if self.is_applier.load(Ordering::Acquire) {
            return Ok(());
        }
        let view = self.follower.view();
        let steal = steal_after.as_millis() as u64;
        let now = self.now();
        let stalled = view.tails.since_ms.is_some_and(|t| now >= t + steal)
            && now >= view.manifest.created_at_ms + steal;
        if view.manifest_id == ManifestId(0) || stalled {
            self.claim().await?;
        }
        Ok(())
    }

    fn should_checkpoint(&self) -> bool {
        let view = self.follower.view();
        let interval = self.cfg.checkpoint_interval.as_millis() as u64;
        view.tails.mem().bytes() >= self.cfg.checkpoint_bytes
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
    async fn lease_floor(&self) -> Result<u64> {
        let store = self.store();
        let now = self.now();
        let writers = store.list_writers().await?;
        self.follower.track(writers.iter().copied());
        let leases =
            futures::future::try_join_all(writers.into_iter().map(|w| wal::read_lease(store, w)))
                .await?;
        Ok(leases
            .into_iter()
            .flatten()
            .filter(|l| l.expires_at_ms > now)
            .map(|l| l.floor_hlc)
            .chain([self.floor_hlc()])
            .min()
            .expect("chained with our own floor"))
    }

    /// Folds every pulled log entry into the tree and publishes a manifest.
    async fn checkpoint(&self) -> Result<()> {
        let mut guard = self.applier.lock().await;
        let Some(app) = guard.as_mut() else {
            return Err(Error::NotApplier);
        };
        let started = Instant::now();
        let floor = self.lease_floor().await?;
        self.follower.refresh(false, self.now()).await?;
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
        let seg = app.next_segment;
        // A fresh segment per attempt: a failed attempt may still have
        // uploaded its segment, which then shows up as an orphan.
        app.next_segment = seg.next();
        let msgs = view.tails.mem().to_sorted_vec();
        let msg_count = msgs.len();
        let plan = tree::apply(
            &self.nodes,
            &self.cfg.tree,
            seg,
            view.manifest.tree,
            msgs,
            &victims,
            drop_below,
        )
        .await?;
        if let Some(segment) = &plan.segment {
            retry("segment upload", || self.nodes.upload(segment)).await?;
        }
        let new = plan.segment.as_ref().map(|s| (s.id, s.data.len() as u64));
        let (table, dead) = segments::account(&view.manifest.segments, new, &plan.freed);
        let next = Manifest {
            applier_epoch: app.epoch,
            tree: plan.root,
            frontier: view.tails.frontier(&view.manifest.frontier),
            segment_watermark: seg.next(),
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
                tracing::debug!(
                    manifest = next_id.0,
                    msgs = msg_count,
                    segment_bytes = new.map_or(0, |(_, b)| b),
                    freed = plan.freed.len(),
                    victims = victims.len(),
                    elapsed = ?started.elapsed(),
                    "checkpoint published"
                );
                self.nodes.evict(&plan.freed);
                self.follower.install(next_id, Arc::new(next), self.now());
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

impl Db {
    pub async fn open(
        os: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
        cfg: DbConfig,
    ) -> Result<Self> {
        Self::open_with_clock(os, path, cfg, Arc::new(SystemClock)).await
    }

    pub async fn open_with_clock(
        os: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
        cfg: DbConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let nodes = Arc::new(Nodes::new(Store::new(os, path), cfg.cache));
        let follower =
            Follower::open(nodes.clone(), cfg.warm_inner_nodes, cfg.rewarm_on_refresh).await?;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            nodes,
            follower,
            writer: random_writer_id(clock.now_ms()),
            hlc: Hlc::default(),
            inflight: Mutex::new(BTreeMap::new()),
            lease_valid_until_ms: AtomicU64::new(0),
            applier: tokio::sync::Mutex::new(None),
            is_applier: AtomicBool::new(false),
            fatal: OnceLock::new(),
            shutdown,
            wake_applier: Notify::new(),
            clock,
            cfg,
        });
        shared.renew_lease().await?;
        shared.follower.refresh(true, shared.now()).await?;
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
        if let Some(gc_cfg) = shared.cfg.gc {
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

    pub async fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<()> {
        self.write(WriteBatch::new().put(key, value), Durability::AwaitDurable)
            .await
    }

    pub async fn delete(&self, key: impl Into<Bytes>) -> Result<()> {
        self.write(WriteBatch::new().delete(key), Durability::AwaitDurable)
            .await
    }

    pub async fn write(&self, batch: WriteBatch, durability: Durability) -> Result<()> {
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let version = self
            .shared
            .follower
            .snapshot()
            .get(&self.shared.nodes, key)
            .await?;
        // Later writes from this process must order after what it has seen.
        if let Some(v) = &version {
            self.shared.hlc.observe(v.ts.hlc);
        }
        Ok(version.and_then(|v| v.value().cloned()))
    }

    pub fn scan(&self, range: impl Into<KeyRange>) -> DbIterator {
        self.shared
            .follower
            .snapshot()
            .scan(self.shared.nodes.clone(), range.into())
    }

    /// Pulls other writers' entries and the latest manifest now.
    pub async fn refresh(&self) -> Result<()> {
        self.shared.follower.refresh(true, self.shared.now()).await
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
        gc::run(self.shared.store(), self.shared.now(), retention).await
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

    pub fn stats(&self) -> DbStats {
        let v = self.shared.follower.view();
        DbStats {
            writer: self.shared.writer,
            is_applier: self.is_applier(),
            applier_epoch: v.manifest.applier_epoch,
            manifest_id: v.manifest_id,
            tree: v.manifest.tree,
            unapplied_bytes: v.tails.mem().bytes(),
            segments: v.manifest.segments.len(),
            segment_bytes: v.manifest.segments.values().map(|u| u.total).sum(),
            live_bytes: v.manifest.segments.values().map(|u| u.live).sum(),
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
        if self.is_applier() {
            match self.shared.checkpoint().await {
                Ok(()) | Err(Error::NotApplier) => {}
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
        let mut tick = tokio::time::interval(self.shared.cfg.wal_flush_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(WalCmd::Write { hlc, ops, done }) => {
                        self.bytes += ops.iter().map(|(k, v)| entry_bytes(k, v.msg.encoded_len())).sum::<usize>();
                        self.ops.extend(ops);
                        self.stamps.push(hlc);
                        self.waiters.extend(done);
                        if self.bytes >= self.shared.cfg.wal_max_bytes {
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
        let result = self.persist(ops).await;
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

    async fn persist(&mut self, ops: Vec<(Bytes, Version)>) -> Result<()> {
        let shared = &self.shared;
        shared.check()?;
        if ops.is_empty() {
            return Ok(());
        }
        // Backpressure: wait while the applier is far behind.
        loop {
            let installed = shared.follower.installed.notified();
            shared.check()?;
            if shared.follower.tails_bytes() < shared.cfg.max_unapplied_bytes {
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
        match retry("wal write", || wal::write(shared.store(), seq, &batch)).await {
            Ok(WalWrite::Written) => {}
            Ok(WalWrite::Taken) => return Err(shared.fail(Error::WriterIdTaken)),
            // The outcome is unknown, so the next batch can't safely reuse
            // this sequence number: stop writing.
            Err(e) => return Err(shared.fail(e)),
        }
        self.next_seq += 1;
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
    while every(shared.cfg.poll_interval, &mut shutdown).await {
        let discover = last_discovery.elapsed() >= shared.cfg.discovery_interval;
        if discover {
            last_discovery = Instant::now();
        }
        if let Err(e) = shared.follower.refresh(discover, shared.now()).await {
            tracing::warn!(%e, "refresh failed");
            continue;
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
            _ = tokio::time::sleep(shared.cfg.poll_interval) => {}
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
        match gc::run(shared.store(), shared.now(), cfg.retention).await {
            Ok(stats) => tracing::debug!(?stats, "gc"),
            Err(e) => tracing::warn!(%e, "gc failed"),
        }
    }
}
