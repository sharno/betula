//! Tracks the latest manifest and every writer's unapplied log tail. Shared by
//! writers (which also add their own batches directly) and readers.
//!
//! Polling uses GETs on the next expected id (manifests and log entries are
//! numbered consecutively), which cost far less than LISTs. LIST runs only on
//! discovery, to find new writers and to catch up after GC. A sealed log
//! (see `wal`) ends at its seal; once applied, it isn't polled again.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::hlc::WriterId;
use crate::logs::Logs;
use crate::manifest::{self, Manifest};

use crate::nodes::{Nodes, Warm};
use crate::protocol::follower::{LogEnd, LogRead, Reading, Sealing, Tracked};
pub use crate::protocol::follower::{Scope, Tails, View};
use crate::snapshot::Snapshot;
use crate::store::Store;
use crate::types::ManifestId;
use crate::wal::{self, SealAt, WalBatch};

/// What a refresh brought in.
#[derive(Debug, Default)]
pub struct Pulled {
    pub changed: bool,
    /// Writers whose logs couldn't be read (their store is down). Their
    /// entries stay where they were; everyone else's came in.
    pub unreachable: Vec<(WriterId, Error)>,
}

impl Pulled {
    /// Fails if any log was unreachable: for reads that promise every write.
    pub fn complete(self) -> Result<()> {
        match self.unreachable.into_iter().next() {
            Some((_, e)) => Err(e),
            None => Ok(()),
        }
    }
}

pub struct Follower {
    nodes: Arc<Nodes>,
    logs: Arc<Logs>,
    rewarm: Warm,
    view: RwLock<View>,
    writers: Mutex<Tracked>,
    /// This process's own writer, whose batches arrive via `add_own`.
    own: Option<WriterId>,
    refreshing: tokio::sync::Mutex<()>,
    /// Notified whenever a new manifest is installed.
    pub installed: Notify,
}

impl Follower {
    /// `warm` loads the tree's internal nodes now; `rewarm` does it again
    /// for every new manifest (worth it only for processes serving reads).
    pub async fn open(
        nodes: Arc<Nodes>,
        logs: Arc<Logs>,
        warm: Warm,
        rewarm: Warm,
        own: Option<WriterId>,
    ) -> Result<Self> {
        let store = nodes.store();
        // An empty database reads as the initial manifest under id 0, which
        // any published manifest supersedes.
        let (manifest_id, manifest) = manifest::load_latest(store)
            .await?
            .unwrap_or_else(|| (ManifestId(0), Manifest::initial(0)));
        nodes.warm(manifest.tree, warm).await?;
        Ok(Self {
            nodes,
            logs,
            rewarm,
            view: RwLock::new(View {
                manifest_id,
                manifest: Arc::new(manifest),
                tails: Tails::default(),
            }),
            writers: Mutex::new(Tracked::default()),
            own,
            refreshing: tokio::sync::Mutex::new(()),
            installed: Notify::new(),
        })
    }

    pub fn logs(&self) -> &Logs {
        &self.logs
    }

    pub fn manifest(&self) -> Arc<Manifest> {
        self.view.read().manifest.clone()
    }

    pub fn view(&self) -> View {
        self.view.read().clone()
    }

    pub fn snapshot(&self) -> Snapshot {
        let v = self.view.read();
        Snapshot {
            tails: v.tails.mem().clone(),
            tree: v.manifest.tree,
        }
    }

    /// See `Tails::has_room`.
    pub fn has_room(&self, me: WriterId, limit: usize) -> bool {
        self.view.read().tails.has_room(me, limit)
    }

    /// Adds this process's own durable batch without waiting for a poll.
    pub fn add_own(&self, seq: u64, batch: Arc<WalBatch>, now_ms: u64) {
        self.view.write().add_own(seq, batch, now_ms);
    }

    /// Installs a manifest this process published.
    pub fn install(&self, id: ManifestId, manifest: Arc<Manifest>, now_ms: u64) {
        self.view.write().install(id, manifest, now_ms);
        self.installed.notify_waiters();
    }

    /// Makes later refreshes pull these writers' logs.
    pub fn track(&self, writers: impl IntoIterator<Item = WriterId>) {
        self.writers.lock().track(writers);
    }

    /// Pulls new manifests and, for `Scope::AllWriters`, other writers' log
    /// entries. `discover` also LISTs, to find new writers and to catch up
    /// when consecutive probing can't. One writer's unreachable log store
    /// doesn't hold up the others (see `Pulled`).
    pub async fn refresh(&self, discover: bool, scope: Scope, now_ms: u64) -> Result<Pulled> {
        let _guard = self.refreshing.lock().await;
        let store = self.nodes.store();
        if discover {
            let frontier = self.view.read().manifest.frontier.clone();
            let before = self.writers.lock().pulled(&frontier);
            let found = store.list_writers().await?.into_iter().collect();
            let forgotten = self.writers.lock().listed(&before, &frontier, found);
            self.logs.forget(&forgotten);
        }

        let current = self.view.read().manifest_id;
        let latest = match discover {
            true => store.list_manifests().await?.last().copied(),
            false => None,
        };
        let newer = match latest.filter(|id| *id > current) {
            Some(id) => load_required(store, id).await?.map(|m| (id, m)),
            None => probe_manifests(store, current).await?,
        };
        if let Some((_, m)) = &newer {
            self.nodes.warm(m.tree, self.rewarm).await?;
        }

        let starts: Vec<(WriterId, u64)> = {
            let v = self.view.read();
            let m = newer.as_ref().map_or(v.manifest.as_ref(), |(_, m)| m);
            v.reads(m, &self.writers.lock(), self.own, scope)
        };
        let results = futures::future::join_all(starts.into_iter().map(|(w, from)| async move {
            let read = async { read_log_from(self.logs.of(w).await?, w, from).await };
            (w, read.await)
        }))
        .await;
        // Store failures (after retries) mean that store is down; anything
        // else, such as corruption, fails the refresh.
        let mut unreachable = Vec::new();
        let mut fresh = Vec::new();
        for (w, r) in results {
            match r {
                Ok(read) => fresh.push((w, read)),
                Err(e) if e.is_retryable() => {
                    tracing::warn!(writer = %w, %e, "writer's log unreachable");
                    unreachable.push((w, e));
                }
                Err(e) => return Err(e),
            }
        }

        let changed = newer.is_some()
            || fresh
                .iter()
                .any(|(_, r)| !r.entries.is_empty() || matches!(r.end, LogEnd::Sealed { .. }));
        let mut v = self.view.write();
        if let Some((id, m)) = newer {
            v.install(id, Arc::new(m), now_ms);
        }
        fresh
            .into_iter()
            .for_each(|(w, read)| v.absorb(w, read, now_ms));
        drop(v);
        self.installed.notify_waiters();
        Ok(Pulled {
            changed,
            unreachable,
        })
    }

    /// Seals `writer`'s log right after its last entry and pulls every entry
    /// before the seal. A batch that lands first moves the seal past it.
    /// Afterwards `view().is_sealed(writer)`.
    pub async fn seal(&self, writer: WriterId, now_ms: u64) -> Result<()> {
        let _guard = self.refreshing.lock().await;
        let from = {
            let v = self.view.read();
            if v.is_sealed(writer) {
                return Ok(());
            }
            v.tails.next_seq(writer, &v.manifest.frontier)
        };
        let store = self.logs.of(writer).await?;
        let mut sealing = Sealing::from(from);
        let read = loop {
            sealing = match sealing {
                Sealing::Reading(r) => {
                    let record = wal::read(store, writer, r.next()).await?;
                    Sealing::Reading(r).record(record)
                }
                Sealing::Placing { at, .. } => match wal::seal(store, writer, at).await? {
                    SealAt::Sealed => sealing.sealed(),
                    SealAt::Batch(b) => sealing.lost(b),
                },
                Sealing::Done(read) => break read,
            };
        };
        self.view.write().absorb(writer, read, now_ms);
        Ok(())
    }
}

async fn load_required(store: &crate::store::Store, id: ManifestId) -> Result<Option<Manifest>> {
    manifest::load(store, id)
        .await?
        .map(Some)
        .ok_or_else(|| Error::Corrupt {
            path: store.manifest_path(id).to_string(),
            reason: "listed manifest is missing".into(),
        })
}

/// The newest manifest after `current`, probing consecutive ids.
async fn probe_manifests(
    store: &crate::store::Store,
    current: ManifestId,
) -> Result<Option<(ManifestId, Manifest)>> {
    let mut newest = None;
    let mut id = current.next();
    while let Some(m) = manifest::load(store, id).await? {
        newest = Some((id, m));
        id = id.next();
    }
    Ok(newest)
}

/// Consecutive log entries of `writer` from `from`, up to the first gap or
/// the seal.
async fn read_log_from(store: &Store, writer: WriterId, from: u64) -> Result<LogRead> {
    let mut reading = Reading::from(from);
    loop {
        let record = wal::read(store, writer, reading.next()).await?;
        reading = match reading.record(record) {
            Ok(read) => return Ok(read),
            Err(more) => more,
        };
    }
}
