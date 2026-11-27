//! The writer. One process at a time writes a database; opening a writer
//! fences the previous one.
//!
//! Write path: each write gets a hybrid-clock timestamp and goes into the
//! log (`wal/{seq}`, put-if-absent at consecutive slots). Once durable it is
//! acknowledged and visible here; readers see it after their next poll.
//!
//! The writer also folds the log into the tree in checkpoints
//! (copy-on-write, one segment object per checkpoint) and publishes each as
//! the next manifest with put-if-absent.
//!
//! Opening claims the next writer epoch by publishing a manifest, then puts
//! a fence in the first empty log slot and replays the log up to it. The
//! previous writer's next log put lands on the fence and its next manifest
//! conflicts, so it stops for good (`Error::Fenced`) without acknowledging
//! anything the new writer doesn't have.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::RwLock;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
// tokio's, so simulations with paused time control it.
use tokio::time::{Instant, MissedTickBehavior};

use crate::error::{Error, Result};
use crate::follow::View;
use crate::gc::{self, GcConfig, GcStats};
use crate::hlc::{Hlc, Ts};
use crate::location;
use crate::manifest::{self, Manifest, Publish};
use crate::nodes::{CacheStats, Nodes};
use crate::protocol::checkpoint;
use crate::protocol::fence::{self, AfterPlace, AfterRead, Fenced, Superseded};
use crate::protocol::log::{self, Gate, LogWriter, PutOutcome, Stopped};
use crate::segments::{self, SegmentKind, SegmentTable};
use crate::settings::{Gc, Settings};
use crate::snapshot::DbIterator;
use crate::store::{RequestStats, Store};
use crate::tree::{self, TreeShape};
use crate::types::{
    Epoch, KeyRange, ManifestId, Msg, SegmentId, TreeRoot, Version, WalSeq, WriterId,
};
use crate::wal::{self, WalWrite};
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
    pub epoch: Epoch,
    pub manifest_id: ManifestId,
    /// Time since the current manifest was published.
    pub manifest_age: Duration,
    pub tree: Option<TreeRoot>,
    /// Durable writes the tree doesn't hold yet.
    pub unapplied_bytes: usize,
    /// How long the oldest of them has waited.
    pub unapplied_age: Option<Duration>,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
    /// Batches made durable, and the log objects holding them.
    pub batches_written: u64,
    pub log_flushes: u64,
    /// Checkpoints published, and the log bytes they applied.
    pub checkpoints: u64,
    pub applied_bytes: u64,
    pub last_checkpoint: Option<Duration>,
    pub cache: CacheStats,
    pub requests: RequestStats,
}

#[derive(Debug, Default)]
struct Counters {
    batches: AtomicU64,
    flushes: AtomicU64,
    checkpoints: AtomicU64,
    applied_bytes: AtomicU64,
    /// Duration of the last checkpoint in microseconds plus one (0: none).
    last_checkpoint_us: AtomicU64,
}

/// Whether a checkpoint left batches for the next one
/// (`CheckpointConfig::max_bytes`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    Nothing,
    Batches,
}

struct Shared {
    nodes: Arc<Nodes>,
    cfg: Settings,
    clock: Arc<dyn Clock>,
    epoch: Epoch,
    hlc: Hlc,
    view: RwLock<View>,
    /// The next segment id, held for a whole checkpoint so checkpoints don't
    /// overlap.
    checkpointing: tokio::sync::Mutex<SegmentId>,
    /// Set once this writer can no longer write.
    fatal: OnceLock<Error>,
    shutdown: watch::Sender<bool>,
    /// A batch became durable.
    durable: Notify,
    /// A checkpoint published, so backpressure may have eased.
    published: Notify,
    counters: Counters,
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
        self.published.notify_waiters();
        e
    }

    /// Whether a checkpoint is due by size, age, or tombstones to purge.
    fn checkpoint_due(&self) -> bool {
        let v = self.view.read();
        checkpoint::due(&self.cfg.checkpoint, &v.tail, self.now())
            || tree::purge_due(
                v.manifest.tree.and_then(|r| r.purge_at),
                self.hlc.now(self.now()),
                self.cfg.tree.purge_tombstones_after,
            )
    }

    /// Checkpoints until no batch is left.
    async fn checkpoint_all(&self) -> Result<()> {
        while self.checkpoint().await? == Left::Batches {}
        Ok(())
    }

    /// Folds the oldest log batches, up to `CheckpointConfig::max_bytes`,
    /// into the tree and publishes a manifest.
    async fn checkpoint(&self) -> Result<Left> {
        let mut next_segment = self.checkpointing.lock().await;
        self.check()?;
        let started = Instant::now();
        let view = self.view.read().clone();
        // Above every timestamp in the tree and the log: every tombstone a
        // checkpoint carries to a leaf goes.
        let now = self.hlc.now(self.now());
        let Some(work) = checkpoint::plan(
            &view.tail,
            &view.manifest,
            self.cfg.checkpoint.max_bytes,
            &self.cfg.cleaner,
            self.cfg.tree.purge_tombstones_after,
            now,
        ) else {
            return Ok(Left::Nothing);
        };
        let out = tree::Outputs {
            inner: *next_segment,
            leaves: next_segment.next(),
        };
        // Fresh segments per attempt: a failed attempt may still have
        // uploaded its segments, which then show up as orphans.
        *next_segment = out.leaves.next();
        let (msgs, msg_bytes, left) = match &work.taken {
            Some(t) => (t.msgs.clone(), t.bytes, t.left),
            None => (Vec::new(), 0, false),
        };
        let msg_count = msgs.len();
        let misses_before = self.nodes.cache_stats().misses;
        let ck = tree::Checkpoint {
            out,
            victims: &work.victims,
            now,
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
        let next = checkpoint::next_manifest(
            &view.manifest,
            self.epoch,
            work.taken.as_ref(),
            checkpoint::Applied {
                tree: plan.root,
                segments: table,
                dead,
                segment_watermark: *next_segment,
            },
            self.now(),
        );
        let next_id = view.manifest_id.next();
        match manifest::publish(self.store(), next_id, &next).await? {
            // A checkpoint's manifest names its writer's epoch, so a match
            // is our own attempt whose response was lost. A writer stalled
            // past the GC retention may have put it into an id GC
            // collected: a newer manifest is then listed, and this one is a
            // stray GC deletes.
            Publish::Published | Publish::Matched
                if !self.store().manifest_above(next_id).await? =>
            {
                let bytes_of = |id: SegmentId| {
                    new.iter()
                        .find(|(s, _, _)| *s == id)
                        .map_or(0, |(_, b, _)| *b)
                };
                let s = &plan.stats;
                tracing::debug!(
                    manifest = next_id.0,
                    msgs = msg_count,
                    msg_bytes,
                    inner_bytes = bytes_of(out.inner),
                    leaf_bytes = bytes_of(out.leaves),
                    freed = plan.freed.len(),
                    victims = work.victims.len(),
                    node_loads,
                    run_bytes = s.run_bytes,
                    internal_bytes = s.internal_bytes,
                    relocated_run_bytes = s.relocated_run_bytes,
                    leaves_rewritten = s.leaves_rewritten,
                    leaf_msgs = s.leaf_msgs,
                    leaves_moved = s.leaves_moved,
                    leaves_underfull = s.leaves_underfull,
                    loaded_bytes = s.loaded_bytes,
                    purge_bytes = s.purge_bytes,
                    // Phase durations in ms: tree rewrite (including node
                    // loads), segment upload, publish.
                    plan_ms = planned.as_millis(),
                    upload_ms = (uploaded - planned).as_millis(),
                    publish_ms = (started.elapsed() - uploaded).as_millis(),
                    elapsed = ?started.elapsed(),
                    "checkpoint published"
                );
                self.nodes.evict(&plan.freed);
                self.view
                    .write()
                    .install(next_id, Arc::new(next), self.now());
                self.published.notify_waiters();
                let c = &self.counters;
                c.checkpoints.fetch_add(1, Ordering::Relaxed);
                c.applied_bytes
                    .fetch_add(msg_bytes as u64, Ordering::Relaxed);
                let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX - 1);
                c.last_checkpoint_us.store(us + 1, Ordering::Relaxed);
                Ok(match left {
                    true => Left::Batches,
                    false => Left::Nothing,
                })
            }
            Publish::Published | Publish::Matched | Publish::Conflict => {
                Err(self.fail(Error::Fenced))
            }
        }
    }

    /// Fails with `Fenced` if a newer writer published a manifest.
    async fn check_fenced(&self) -> Result<()> {
        // Held so our own checkpoint can't publish between the read and
        // the listing.
        let _checkpointing = self.checkpointing.lock().await;
        let id = self.view.read().manifest_id;
        match self.store().manifest_above(id).await? {
            true => Err(self.fail(Error::Fenced)),
            false => Ok(()),
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
    /// Held while stamping a batch and queueing it, so log order is
    /// timestamp order: a checkpoint that applies a prefix of the log then
    /// never leaves an older version of a key it dropped a tombstone for.
    cmd: Option<tokio::sync::Mutex<mpsc::Sender<WalCmd>>>,
    tasks: Vec<JoinHandle<()>>,
}

/// Configures and opens a writer. See [`Db::builder`].
pub struct DbBuilder {
    store: Arc<dyn ObjectStore>,
    path: Path,
    settings: Settings,
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

    pub async fn build(self) -> Result<Db> {
        Db::start(self.store, self.path, self.settings, self.clock).await
    }
}

/// Publishes the next manifest with the next writer epoch, for opening
/// `writer`. A concurrent opener that publishes first just means trying
/// again on top of it: the last writer to open wins.
async fn claim(store: &Store, writer: WriterId, now_ms: u64) -> Result<(ManifestId, Manifest)> {
    loop {
        let (id, current) = manifest::load_latest(store)
            .await?
            .unwrap_or_else(|| (ManifestId(0), Manifest::initial(now_ms)));
        let next = current.claimed(writer, now_ms);
        match manifest::publish(store, id.next(), &next).await? {
            // The claim names this opening, so a match is our own put whose
            // response was lost. A newer manifest listed above it means
            // this opening lost: a newer opener claimed past it, or it
            // stalled past the GC retention and put its claim into an id
            // GC collected (a stray GC deletes). Claiming again instead
            // would let two openers supersede each other forever.
            Publish::Published | Publish::Matched => {
                return match store.manifest_above(id.next()).await? {
                    false => Ok((id.next(), next)),
                    true => {
                        tracing::info!(epoch = next.writer_epoch.0, "superseded while claiming");
                        Err(Error::Fenced)
                    }
                };
            }
            Publish::Conflict => {}
        }
    }
}

/// Fences the log after the claimed manifest's applied slot (see
/// `protocol::fence`), for the writer that claimed manifest `claimed`.
async fn fence_log(
    store: &Store,
    claimed: ManifestId,
    epoch: Epoch,
    applied: WalSeq,
) -> Result<Fenced> {
    let superseded = |Superseded(by): Superseded| {
        tracing::info!(epoch = epoch.0, by = by.0, "superseded while opening");
        Error::Fenced
    };
    let mut reading = fence::Reading::new(epoch, applied);
    loop {
        let record = wal::read(store, reading.next()).await?;
        let placing = match reading.record(record).map_err(superseded)? {
            AfterRead::More(r) => {
                reading = r;
                continue;
            }
            AfterRead::Place(p) => p,
        };
        let outcome = wal::fence(store, placing.at(), placing.epoch()).await?;
        reading = match placing.outcome(outcome).map_err(superseded)? {
            AfterPlace::Done(fenced) => {
                // An opener stalled between reading an empty slot and
                // fencing it may have been overtaken: a newer writer
                // applied that slot and GC deleted it, so the fence landed
                // where no reader looks. A newer writer claims a later
                // manifest; any listed now means this one is out.
                return match store.manifest_above(claimed).await? {
                    true => {
                        tracing::info!(epoch = epoch.0, "superseded while fencing");
                        Err(Error::Fenced)
                    }
                    false => Ok(fenced),
                };
            }
            AfterPlace::More(r) => r,
        };
    }
}

impl Db {
    /// Opens a writer at a URL such as `s3://bucket/path`, `gs://`, `az://`,
    /// `file:///tmp/db` or `memory:///`, fencing any writer already open.
    /// Credentials and endpoints come from the usual environment variables
    /// (`AWS_*`, `GOOGLE_*`, `AZURE_*`); settings from [`Settings::load`].
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
            clock: Arc::new(SystemClock),
        }
    }

    async fn start(
        os: Arc<dyn ObjectStore>,
        path: Path,
        cfg: Settings,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let store = Store::new(os, path, cfg.hedge);
        // Random per opening (std's per-process random keys): openers on
        // the same manifest share an epoch, and GC tells them apart by this.
        let writer = WriterId(RandomState::new().hash_one(clock.now_ms()));
        let (manifest_id, claimed) = claim(&store, writer, clock.now_ms()).await?;
        let epoch = claimed.writer_epoch;
        delete_orphans(&store, claimed.segment_watermark, epoch).await?;
        let Fenced { read, fence } =
            fence_log(&store, manifest_id, epoch, claimed.wal_applied).await?;
        tracing::info!(epoch = epoch.0, fence = fence.0, "opened the writer");
        let nodes = Arc::new(Nodes::new(store, cfg.cache));
        nodes.warm(claimed.tree, cfg.warm).await?;
        let hlc = Hlc::default();
        read.batches
            .iter()
            .flat_map(|(_, b)| b.ops.iter().map(|(_, v)| v.ts))
            .chain([claimed.last_ts])
            .for_each(|ts| hlc.observe(ts));
        let mut view = View::new(manifest_id, claimed);
        let now = clock.now_ms();
        view.tail.absorb(read, now);
        view.tail.add_fence(fence);

        let (shutdown, shutdown_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            nodes,
            epoch,
            hlc,
            view: RwLock::new(view),
            checkpointing: tokio::sync::Mutex::new(SegmentId { epoch, seq: 0 }),
            fatal: OnceLock::new(),
            shutdown,
            durable: Notify::new(),
            published: Notify::new(),
            counters: Counters::default(),
            clock,
            cfg,
        });
        let (cmd, rx) = mpsc::channel(CMD_QUEUE);
        let wal_task = WalTask {
            log: LogWriter::new(epoch, fence.next(), shared.cfg.wal.max_bytes),
            shared: shared.clone(),
        };
        let mut tasks = vec![
            tokio::spawn(wal_task.run(rx)),
            tokio::spawn(checkpoint_loop(shared.clone(), shutdown_rx.clone())),
        ];
        if let Gc::On(gc_cfg) = shared.cfg.gc {
            tasks.push(tokio::spawn(gc_loop(shared.clone(), gc_cfg, shutdown_rx)));
        }
        Ok(Self {
            shared,
            cmd: Some(tokio::sync::Mutex::new(cmd)),
            tasks,
        })
    }

    /// This writer's generation.
    pub fn epoch(&self) -> Epoch {
        self.shared.epoch
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
    /// every op in it carries (`None` for an empty batch).
    pub async fn write(&self, batch: WriteBatch) -> Result<Option<Ts>> {
        self.write_with(batch, WriteOptions::default()).await
    }

    pub async fn write_with(&self, batch: WriteBatch, opts: WriteOptions) -> Result<Option<Ts>> {
        self.shared.check()?;
        if batch.ops.is_empty() {
            return Ok(None);
        }
        // One timestamp per batch, so collapse repeated keys: last op wins.
        let ops: BTreeMap<Bytes, Msg> = batch.ops.into_iter().collect();
        let (done, wait) = match opts.durability {
            Durability::AwaitDurable => {
                let (tx, rx) = oneshot::channel();
                (Some(tx), Some(rx))
            }
            Durability::Queued => (None, None),
        };
        let tx = self.cmd.as_ref().ok_or(Error::Closed)?.lock().await;
        let ts = self.shared.hlc.now(self.shared.now());
        let ops = ops
            .into_iter()
            .map(|(k, msg)| (k, Version { ts, msg }))
            .collect();
        tx.send(WalCmd::Write { ops, done })
            .await
            .map_err(|_| Error::Closed)?;
        drop(tx);
        if let Some(rx) = wait {
            rx.await.map_err(|_| Error::Closed)??;
        }
        Ok(Some(ts))
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let snapshot = self.shared.view.read().snapshot();
        let version = snapshot.get(&self.shared.nodes, key.as_ref()).await?;
        Ok(version.and_then(|v| v.value().cloned()))
    }

    /// Live pairs in `range`, in key order, from a consistent snapshot.
    /// Accepts any range of byte-like keys: `"a".."m"`, `key..`, `..=key`.
    pub fn scan<K: AsRef<[u8]>>(&self, range: impl RangeBounds<K>) -> DbIterator {
        let snapshot = self.shared.view.read().snapshot();
        snapshot.scan(self.shared.nodes.clone(), KeyRange::from_bounds(range))
    }

    /// `Ok` while this writer can write; otherwise the error that stopped it
    /// (for example `Fenced`). Background failures surface here.
    pub fn status(&self) -> Result<()> {
        self.shared.check()
    }

    /// Makes every write sent so far durable.
    pub async fn flush_wal(&self) -> Result<()> {
        let (done, rx) = oneshot::channel();
        let tx = self.cmd.as_ref().ok_or(Error::Closed)?.lock().await;
        tx.send(WalCmd::Flush { done })
            .await
            .map_err(|_| Error::Closed)?;
        drop(tx);
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Folds every durable write into the tree now, in checkpoints of at
    /// most `CheckpointConfig::max_bytes`.
    pub async fn checkpoint(&self) -> Result<()> {
        self.flush_wal().await?;
        self.shared.checkpoint_all().await
    }

    pub async fn run_gc(&self, retention: Duration) -> Result<GcStats> {
        gc::run(
            self.shared.store(),
            self.shared.now(),
            retention,
            gc::Speed::Full,
        )
        .await
    }

    /// Walks the current tree, loading every node.
    pub async fn tree_shape(&self) -> Result<TreeShape> {
        let root = self.shared.view.read().manifest.tree;
        tree::shape(&self.shared.nodes, root).await
    }

    /// The current tree's shape with its manifest's segment table, both
    /// from one manifest (separate calls can straddle a checkpoint).
    pub async fn audit(&self) -> Result<Audit> {
        let m = self.shared.view.read().manifest.clone();
        Ok(Audit {
            shape: tree::shape(&self.shared.nodes, m.tree).await?,
            segments: m.segments.clone(),
        })
    }

    /// Live and total bytes of every segment the current tree references.
    pub fn segment_table(&self) -> SegmentTable {
        self.shared.view.read().manifest.segments.clone()
    }

    pub fn metrics(&self) -> Metrics {
        let v = self.shared.view.read().clone();
        let now = self.shared.now();
        let ms = |t: u64| Duration::from_millis(now.saturating_sub(t));
        let c = &self.shared.counters;
        Metrics {
            epoch: self.shared.epoch,
            manifest_id: v.manifest_id,
            manifest_age: ms(v.manifest.created_at_ms),
            tree: v.manifest.tree,
            unapplied_bytes: v.tail.retained_bytes(),
            unapplied_age: v.tail.since_ms.map(ms),
            segments: v.manifest.segments.len(),
            segment_bytes: v.manifest.segments.values().map(|u| u.total).sum(),
            live_bytes: v.manifest.segments.values().map(|u| u.live).sum(),
            batches_written: c.batches.load(Ordering::Relaxed),
            log_flushes: c.flushes.load(Ordering::Relaxed),
            checkpoints: c.checkpoints.load(Ordering::Relaxed),
            applied_bytes: c.applied_bytes.load(Ordering::Relaxed),
            last_checkpoint: c
                .last_checkpoint_us
                .load(Ordering::Relaxed)
                .checked_sub(1)
                .map(Duration::from_micros),
            cache: self.shared.nodes.cache_stats(),
            requests: self.shared.store().requests(),
        }
    }

    /// Makes pending writes durable, applies them, and stops background
    /// work. Writes that are durable but not applied (a failed final
    /// checkpoint) are applied by the next writer.
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
        match self.shared.checkpoint_all().await {
            Ok(()) | Err(Error::Fenced) => {}
            Err(e) => tracing::warn!(%e, "final checkpoint failed"),
        }
        self.shared.check()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if self.cmd.is_some() {
            tracing::warn!(
                epoch = self.shared.epoch.0,
                "Db dropped without close(): queued (not yet durable) writes may be lost; \
                 durable ones are applied by the next writer"
            );
        }
        self.shared.shutdown.send_replace(true);
    }
}

/// Segments at or above the published watermark from earlier epochs were
/// uploaded by a writer that died (or was fenced) before publishing.
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
    std::pin::Pin<Box<dyn std::future::Future<Output = (WalSeq, Result<WalWrite>)> + Send>>;

impl WalTask {
    fn gate(&self) -> Gate {
        let s = &self.shared;
        match s
            .view
            .read()
            .tail
            .has_room(s.cfg.checkpoint.max_unapplied_bytes)
        {
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
            let published = self.shared.published.notified();
            // Biased, so a seed replays the same order (`select!` otherwise
            // polls in random order). Commands come last: under load they're
            // always ready and would starve the rest.
            let event = tokio::select! {
                biased;
                (seq, result) = async { putting.as_mut().expect("guarded").await }, if putting.is_some() => {
                    putting = None;
                    let outcome = match result {
                        Ok(WalWrite::Written) => PutOutcome::Written,
                        Ok(WalWrite::Fenced) => PutOutcome::Fenced,
                        Err(e) => {
                            failure = Some(e);
                            PutOutcome::Failed
                        }
                    };
                    log::Event::PutDone { seq, outcome }
                },
                // Only while writes wait: an idle writer doesn't wake every
                // flush interval (and a simulation's cost follows its work,
                // not its virtual time). A write after a quiet spell finds
                // the tick overdue and goes out at once.
                _ = tick.tick(), if self.log.has_pending() => log::Event::Tick,
                _ = published, if self.log.held_back() => log::Event::Retry,
                cmd = rx.recv(), if open => match cmd {
                    Some(WalCmd::Write { ops, done }) => log::Event::Write { ops, waiter: done },
                    Some(WalCmd::Flush { done }) => log::Event::Flush { waiter: done },
                    None => {
                        open = false;
                        log::Event::Close
                    }
                },
            };
            let gate = self.gate();
            for effect in self.log.step(event, gate) {
                match effect {
                    log::Effect::Put { seq, batch } => {
                        let (store, compression) =
                            (self.shared.store().clone(), self.shared.cfg.compression);
                        putting = Some(Box::pin(async move {
                            (seq, wal::write(&store, seq, &batch, compression).await)
                        }));
                    }
                    log::Effect::Durable { seq, batch, writes } => {
                        let s = &self.shared;
                        let c = &s.counters;
                        c.flushes.fetch_add(1, Ordering::Relaxed);
                        c.batches.fetch_add(writes, Ordering::Relaxed);
                        s.view.write().tail.add(seq, batch, s.now());
                        s.durable.notify_one();
                    }
                    log::Effect::Ack { waiters, result } => {
                        let result = result.map_err(|why| self.stopped_error(why, &failure));
                        waiters.into_iter().for_each(|w| {
                            let _ = w.send(result.clone());
                        });
                    }
                    log::Effect::Stopped { why } => {
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
            (Stopped::Fenced, _) => Error::Fenced,
            (Stopped::Unknown, Some(e)) => Error::LogWriteFailed(Arc::new(e.clone())),
            (Stopped::Unknown, None) => Error::Closed,
        }
    }
}

// ---------------------------------------------------------------------------
// Background loops

async fn every(period: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        // Biased for replayable simulations; shutdown wins a tie.
        biased;
        _ = shutdown.changed() => false,
        _ = tokio::time::sleep(period) => !*shutdown.borrow(),
    }
}

async fn checkpoint_loop(shared: Arc<Shared>, mut shutdown: watch::Receiver<bool>) {
    let interval = shared.cfg.checkpoint.interval;
    let mut last_fence_check = Instant::now();
    let mut left = Left::Nothing;
    loop {
        // A checkpoint that left batches goes again at once, and so does
        // one due already: batches that arrived during the last one may
        // be all there is, with backpressure holding back new ones.
        if left == Left::Nothing && !shared.checkpoint_due() {
            tokio::select! {
                // Biased for replayable simulations; shutdown wins a tie.
                biased;
                _ = shutdown.changed() => return,
                _ = shared.durable.notified() => {}
                _ = tokio::time::sleep(interval / 4) => {}
            }
        }
        if *shutdown.borrow() || shared.check().is_err() {
            return;
        }
        let result = match shared.checkpoint_due() {
            true => shared.checkpoint().await,
            // An idle writer learns it was fenced before a write fails.
            false if last_fence_check.elapsed() >= interval => {
                last_fence_check = Instant::now();
                shared.check_fenced().await.map(|()| Left::Nothing)
            }
            false => Ok(Left::Nothing),
        };
        left = match result {
            Ok(left) => left,
            Err(Error::Fenced) => return,
            Err(e) => {
                tracing::warn!(%e, "checkpoint failed, retrying");
                let _ = every(Duration::from_secs(1), &mut shutdown).await;
                Left::Nothing
            }
        };
    }
}

async fn gc_loop(shared: Arc<Shared>, cfg: GcConfig, mut shutdown: watch::Receiver<bool>) {
    while every(cfg.interval, &mut shutdown).await && shared.check().is_ok() {
        let speed = gc::Speed::Spread {
            over: cfg.interval,
            max_per_sec: cfg.max_deletes_per_sec,
        };
        match gc::run(shared.store(), shared.now(), cfg.retention, speed).await {
            Ok(stats) => tracing::debug!(?stats, "gc"),
            Err(e) => tracing::warn!(%e, "gc failed"),
        }
    }
}
