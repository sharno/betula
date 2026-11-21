//! The single writer.
//!
//! Write path: ops are batched into WAL objects (put-if-absent, so a fenced
//! writer finds its next id taken). Once a WAL object is durable its ops enter
//! the active memtable and become visible. A full memtable is frozen and the
//! checkpointer applies it to the tree copy-on-write, uploads the new nodes,
//! and publishes the next manifest with put-if-absent.
//!
//! Open: claim the next writer epoch by publishing a manifest, fence the WAL
//! by writing an empty batch at the next free id, replay WALs newer than the
//! manifest, and delete nodes a crashed writer uploaded but never published.

use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::RwLock;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::error::{Error, Result};
use crate::gc::{self, GcConfig, GcStats};
use crate::manifest::{self, Manifest, Publish};
use crate::memtable::Memtable;
use crate::nodes::{CacheConfig, Nodes};
use crate::retry::retry;
use crate::snapshot::{DbIterator, Snapshot};
use crate::store::Store;
use crate::tree::{self, IdAlloc, Plan, TreeConfig, TreeShape};
use crate::types::{Epoch, KeyRange, ManifestId, Msg, NodeId, TreeRoot, WalId, entry_bytes};
use crate::wal::{self, WalBatch, WalWrite};

const REPLAY_CONCURRENCY: usize = 16;
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

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub tree: TreeConfig,
    pub cache: CacheConfig,
    pub wal_flush_interval: Duration,
    /// Flush the WAL early once this many bytes are pending.
    pub wal_max_bytes: usize,
    /// Freeze the active memtable for checkpointing at this size.
    pub memtable_bytes: usize,
    /// Frozen memtables allowed before WAL writes stall.
    pub max_immutables: usize,
    pub gc: Option<GcConfig>,
    /// Load every internal node into cache on open.
    pub warm_inner_nodes: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            tree: TreeConfig::default(),
            cache: CacheConfig::default(),
            wal_flush_interval: Duration::from_millis(50),
            wal_max_bytes: 4 << 20,
            memtable_bytes: 32 << 20,
            max_immutables: 2,
            gc: Some(GcConfig::default()),
            warm_inner_nodes: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Return once the batch is in a durable WAL object (and visible).
    #[default]
    AwaitDurable,
    /// Return once the batch is queued. It becomes durable and visible with
    /// the next WAL flush.
    Queued,
}

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
    pub writer_epoch: Epoch,
    pub manifest_id: ManifestId,
    pub tree: Option<TreeRoot>,
    pub immutables: usize,
    pub active_bytes: usize,
}

/// A frozen memtable covering every WAL id up to `last_wal`.
#[derive(Clone)]
struct Frozen {
    mem: Memtable,
    last_wal: WalId,
}

struct WriterState {
    active: Memtable,
    /// Oldest first.
    immutables: VecDeque<Frozen>,
    manifest_id: ManifestId,
    manifest: Arc<Manifest>,
}

struct Shared {
    nodes: Arc<Nodes>,
    cfg: DbConfig,
    clock: Arc<dyn Clock>,
    state: RwLock<WriterState>,
    imm_added: Notify,
    imm_drained: Notify,
    /// Set once the writer can no longer make progress (fenced or failed).
    fatal: OnceLock<Error>,
    shutdown: watch::Sender<bool>,
}

impl Shared {
    fn store(&self) -> &Store {
        self.nodes.store()
    }

    fn check(&self) -> Result<()> {
        self.fatal.get().map_or(Ok(()), |e| Err(e.clone()))
    }

    fn fail(&self, e: Error) -> Error {
        let e = self.fatal.get_or_init(|| e).clone();
        self.imm_drained.notify_waiters();
        self.imm_added.notify_one();
        e
    }

    fn snapshot(&self) -> Snapshot {
        let s = self.state.read();
        Snapshot {
            mems: std::iter::once(s.active.clone())
                .chain(s.immutables.iter().rev().map(|f| f.mem.clone()))
                .collect(),
            tree: s.manifest.tree,
        }
    }

    fn maybe_freeze(&self, last_wal: WalId, when: Freeze) {
        let mut s = self.state.write();
        let wanted = match when {
            Freeze::Always => true,
            Freeze::WhenFull => s.active.bytes() >= self.cfg.memtable_bytes,
        };
        if s.active.is_empty() || !wanted {
            return;
        }
        let mem = std::mem::take(&mut s.active);
        s.immutables.push_back(Frozen { mem, last_wal });
        drop(s);
        self.imm_added.notify_one();
    }

    /// Waits until `done` holds for the state, re-checking after every
    /// checkpoint.
    async fn wait_until(&self, done: impl Fn(&WriterState) -> bool) -> Result<()> {
        loop {
            let drained = self.imm_drained.notified();
            self.check()?;
            if done(&self.state.read()) {
                return Ok(());
            }
            drained.await;
        }
    }
}

pub struct Db {
    shared: Arc<Shared>,
    cmd: Option<mpsc::Sender<WalCmd>>,
    tasks: Vec<JoinHandle<()>>,
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
        let store = Store::new(os, path);
        let (manifest_id, manifest) = claim(&store, clock.as_ref()).await?;
        let epoch = manifest.writer_epoch;
        let fence = fence_wal(&store, epoch, manifest.wal_replay_after).await?;
        let replayed = replay(&store, manifest.wal_replay_after, fence).await?;
        delete_orphans(&store, manifest.node_watermark, epoch).await?;

        let nodes = Arc::new(Nodes::new(store, cfg.cache));
        if cfg.warm_inner_nodes {
            nodes.warm_inner(manifest.tree).await?;
        }
        // Checkpoint replayed ops right away so the WAL tail stays short.
        let immutables = if replayed.is_empty() {
            VecDeque::new()
        } else {
            VecDeque::from([Frozen {
                mem: replayed,
                last_wal: fence,
            }])
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let shared = Arc::new(Shared {
            nodes,
            clock,
            state: RwLock::new(WriterState {
                active: Memtable::default(),
                immutables,
                manifest_id,
                manifest: Arc::new(manifest),
            }),
            imm_added: Notify::new(),
            imm_drained: Notify::new(),
            fatal: OnceLock::new(),
            shutdown,
            cfg,
        });
        shared.imm_added.notify_one();

        let (cmd, rx) = mpsc::channel(CMD_QUEUE);
        let wal_task = WalTask {
            shared: shared.clone(),
            epoch,
            next: fence.next(),
            ops: Vec::new(),
            waiters: Vec::new(),
            bytes: 0,
        };
        let alloc = IdAlloc::new(NodeId { epoch, seq: 0 });
        let mut tasks = vec![
            tokio::spawn(wal_task.run(rx)),
            tokio::spawn(checkpoint_loop(shared.clone(), alloc, shutdown_rx.clone())),
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
        let (done, wait) = match durability {
            Durability::AwaitDurable => {
                let (tx, rx) = oneshot::channel();
                (Some(tx), Some(rx))
            }
            Durability::Queued => (None, None),
        };
        self.send(WalCmd::Write {
            ops: batch.ops,
            done,
        })
        .await?;
        match wait {
            Some(rx) => rx.await.map_err(|_| Error::Closed)?,
            None => Ok(()),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.shared.snapshot().get(&self.shared.nodes, key).await
    }

    pub fn scan(&self, range: impl Into<KeyRange>) -> DbIterator {
        self.shared
            .snapshot()
            .scan(self.shared.nodes.clone(), range.into())
    }

    /// Makes every write sent so far durable.
    pub async fn flush_wal(&self) -> Result<()> {
        self.flush(Freeze::WhenFull).await
    }

    /// Makes every write sent so far part of a published tree version.
    pub async fn checkpoint(&self) -> Result<()> {
        self.flush(Freeze::Always).await?;
        self.shared.wait_until(|s| s.immutables.is_empty()).await
    }

    pub async fn run_gc(&self, retention: Duration) -> Result<GcStats> {
        gc::run(self.shared.store(), self.shared.clock.now_ms(), retention).await
    }

    /// Walks the current tree, loading every node.
    pub async fn tree_shape(&self) -> Result<TreeShape> {
        let root = self.shared.state.read().manifest.tree;
        tree::shape(&self.shared.nodes, root).await
    }

    pub fn stats(&self) -> DbStats {
        let s = self.shared.state.read();
        DbStats {
            writer_epoch: s.manifest.writer_epoch,
            manifest_id: s.manifest_id,
            tree: s.manifest.tree,
            immutables: s.immutables.len(),
            active_bytes: s.active.bytes(),
        }
    }

    /// Flushes pending writes to the WAL and stops background work. Frozen
    /// memtables that weren't checkpointed are replayed on the next open.
    pub async fn close(mut self) -> Result<()> {
        // Dropping the sender makes the WAL task flush and exit.
        self.cmd.take();
        let mut tasks = std::mem::take(&mut self.tasks).into_iter();
        if let Some(wal) = tasks.next() {
            let _ = wal.await;
        }
        self.shared.shutdown.send_replace(true);
        futures::future::join_all(tasks).await;
        match self.shared.fatal.get() {
            Some(Error::Fenced) | None => Ok(()),
            Some(e) => Err(e.clone()),
        }
    }

    async fn flush(&self, freeze: Freeze) -> Result<()> {
        let (done, rx) = oneshot::channel();
        self.send(WalCmd::Flush { freeze, done }).await?;
        rx.await.map_err(|_| Error::Closed)?
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

// ---------------------------------------------------------------------------
// Open

/// Publishes a manifest with the next writer epoch, which fences the previous
/// writer's checkpoints.
async fn claim(store: &Store, clock: &dyn Clock) -> Result<(ManifestId, Manifest)> {
    loop {
        let Some((id, current)) = manifest::load_latest(store).await? else {
            // Losing this race is fine: the winner's manifest is used.
            manifest::publish(store, ManifestId(1), &Manifest::initial(clock.now_ms())).await?;
            continue;
        };
        let next = current.fenced(clock.now_ms());
        if manifest::publish(store, id.next(), &next).await? == Publish::Published {
            return Ok((id.next(), next));
        }
    }
}

/// Claims the next free WAL id with an empty batch so the previous writer's
/// next WAL write conflicts.
async fn fence_wal(store: &Store, epoch: Epoch, replay_after: WalId) -> Result<WalId> {
    let listed = store
        .list_wals()
        .await?
        .last()
        .copied()
        .unwrap_or(WalId::NONE);
    let fence = WalBatch {
        epoch,
        ops: Vec::new(),
    };
    let mut id = listed.max(replay_after).next();
    loop {
        match wal::write(store, id, &fence).await? {
            WalWrite::Written => return Ok(id),
            WalWrite::Taken => id = id.next(),
        }
    }
}

async fn replay(store: &Store, after: WalId, fence: WalId) -> Result<Memtable> {
    let batches: Vec<Option<WalBatch>> = stream::iter((after.0 + 1..fence.0).map(WalId))
        .map(|id| wal::read(store, id))
        .buffered(REPLAY_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(batches
        .into_iter()
        .flatten()
        .fold(Memtable::default(), |mut mem, b| {
            mem.apply(b.ops);
            mem
        }))
}

/// Nodes at or above the published watermark from earlier epochs were
/// uploaded by a writer that died (or was fenced) before publishing them.
async fn delete_orphans(store: &Store, watermark: NodeId, epoch: Epoch) -> Result<()> {
    let orphans: Vec<_> = store
        .list_nodes_from(watermark)
        .await?
        .into_iter()
        .filter(|id| id.epoch < epoch)
        .map(|id| store.node_path(id))
        .collect();
    if !orphans.is_empty() {
        tracing::info!(count = orphans.len(), "deleting orphaned nodes");
    }
    store.delete_all(orphans).await
}

// ---------------------------------------------------------------------------
// WAL

/// When to freeze the active memtable after a WAL flush.
#[derive(Clone, Copy, Debug)]
enum Freeze {
    WhenFull,
    Always,
}

enum WalCmd {
    Write {
        ops: Vec<(Bytes, Msg)>,
        done: Option<oneshot::Sender<Result<()>>>,
    },
    Flush {
        freeze: Freeze,
        done: oneshot::Sender<Result<()>>,
    },
}

struct WalTask {
    shared: Arc<Shared>,
    epoch: Epoch,
    next: WalId,
    ops: Vec<(Bytes, Msg)>,
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
                    Some(WalCmd::Write { ops, done }) => {
                        self.bytes += ops.iter().map(|(k, m)| entry_bytes(k, m.encoded_len())).sum::<usize>();
                        self.ops.extend(ops);
                        self.waiters.extend(done);
                        if self.bytes >= self.shared.cfg.wal_max_bytes {
                            let _ = self.flush(Freeze::WhenFull).await;
                        }
                    }
                    Some(WalCmd::Flush { freeze, done }) => {
                        let _ = done.send(self.flush(freeze).await);
                    }
                    None => {
                        let _ = self.flush(Freeze::WhenFull).await;
                        return;
                    }
                },
                _ = tick.tick() => {
                    if !self.ops.is_empty() {
                        let _ = self.flush(Freeze::WhenFull).await;
                    }
                }
            }
        }
    }

    async fn flush(&mut self, freeze: Freeze) -> Result<()> {
        let ops = std::mem::take(&mut self.ops);
        let waiters = std::mem::take(&mut self.waiters);
        self.bytes = 0;
        let result = self.persist(ops, freeze).await;
        waiters.into_iter().for_each(|w| {
            let _ = w.send(result.clone());
        });
        result
    }

    async fn persist(&mut self, ops: Vec<(Bytes, Msg)>, freeze: Freeze) -> Result<()> {
        self.shared.check()?;
        if !ops.is_empty() {
            let max = self.shared.cfg.max_immutables;
            self.shared.wait_until(|s| s.immutables.len() < max).await?;
            let batch = WalBatch {
                epoch: self.epoch,
                ops,
            };
            let id = self.next;
            let store = self.shared.store();
            match retry("wal write", || wal::write(store, id, &batch)).await {
                Ok(WalWrite::Written) => {}
                Ok(WalWrite::Taken) => return Err(self.shared.fail(Error::Fenced)),
                // The outcome is unknown, so the next batch can't safely reuse
                // this id: stop writing.
                Err(e) => return Err(self.shared.fail(e)),
            }
            self.next = id.next();
            self.shared.state.write().active.apply(batch.ops);
        }
        self.shared.maybe_freeze(WalId(self.next.0 - 1), freeze);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Checkpoint and GC

async fn checkpoint_loop(
    shared: Arc<Shared>,
    mut alloc: IdAlloc,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() || shared.fatal.get().is_some() {
            return;
        }
        let added = shared.imm_added.notified();
        let next = {
            let s = shared.state.read();
            s.immutables
                .front()
                .cloned()
                .map(|f| (f, s.manifest_id, s.manifest.clone()))
        };
        let Some((frozen, id, current)) = next else {
            tokio::select! {
                _ = added => {}
                _ = shutdown.changed() => {}
            }
            continue;
        };
        match checkpoint(&shared, &mut alloc, frozen, id, &current).await {
            Ok((id, manifest)) => {
                shared.nodes.evict(&manifest.freed);
                let mut s = shared.state.write();
                s.manifest_id = id;
                s.manifest = Arc::new(manifest);
                s.immutables.pop_front();
                drop(s);
                shared.imm_drained.notify_waiters();
            }
            Err(Error::Fenced) => {
                shared.fail(Error::Fenced);
                return;
            }
            Err(e) => {
                tracing::warn!(%e, "checkpoint failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => {}
                }
            }
        }
    }
}

async fn checkpoint(
    shared: &Shared,
    alloc: &mut IdAlloc,
    frozen: Frozen,
    id: ManifestId,
    current: &Manifest,
) -> Result<(ManifestId, Manifest)> {
    let started = std::time::Instant::now();
    let msgs = frozen.mem.to_sorted_vec();
    let msg_count = msgs.len();
    let plan = match msgs.is_empty() {
        true => Plan {
            root: current.tree,
            written: Vec::new(),
            freed: Vec::new(),
        },
        false => tree::apply(&shared.nodes, &shared.cfg.tree, alloc, current.tree, msgs).await?,
    };
    let applied = started.elapsed();
    retry("node upload", || shared.nodes.upload(&plan.written)).await?;
    let uploaded = started.elapsed();
    let next = Manifest {
        writer_epoch: current.writer_epoch,
        tree: plan.root,
        wal_replay_after: frozen.last_wal,
        node_watermark: alloc.watermark(),
        freed: plan.freed,
        created_at_ms: shared.clock.now_ms(),
    };
    let next_id = id.next();
    match retry("manifest publish", || {
        manifest::publish(shared.store(), next_id, &next)
    })
    .await?
    {
        Publish::Published => {
            tracing::debug!(
                manifest = next_id.0,
                msgs = msg_count,
                written = plan.written.len(),
                freed = next.freed.len(),
                ?applied,
                ?uploaded,
                total = ?started.elapsed(),
                "checkpoint published"
            );
            Ok((next_id, next))
        }
        Publish::Conflict => Err(Error::Fenced),
    }
}

async fn gc_loop(shared: Arc<Shared>, cfg: GcConfig, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(cfg.interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => return,
        }
        if *shutdown.borrow() || shared.fatal.get().is_some() {
            return;
        }
        match gc::run(shared.store(), shared.clock.now_ms(), cfg.retention).await {
            Ok(stats) => tracing::debug!(?stats, "gc"),
            Err(e) => tracing::warn!(%e, "gc failed"),
        }
    }
}
