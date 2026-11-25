//! A reader's view: the newest manifest it has found and the log batches
//! past it.
//!
//! Polling GETs the next manifest id and the next log slot, both numbered
//! consecutively, which costs far less than LISTs.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::Result;
use crate::manifest::{self, Manifest};
use crate::nodes::{Nodes, Warm};
use crate::protocol::tail::{LogRead, Scan, Tail};
use crate::snapshot::Snapshot;
use crate::store::Store;
use crate::types::ManifestId;
use crate::wal;

/// A manifest and the log batches past it.
#[derive(Clone, Debug)]
pub struct View {
    pub manifest_id: ManifestId,
    pub manifest: Arc<Manifest>,
    pub tail: Tail,
}

impl View {
    pub fn new(manifest_id: ManifestId, manifest: Manifest) -> Self {
        Self {
            tail: Tail::new(manifest.wal_applied),
            manifest_id,
            manifest: Arc::new(manifest),
        }
    }

    /// Moves to a newer manifest, dropping the batches it holds.
    pub fn install(&mut self, id: ManifestId, manifest: Arc<Manifest>, now_ms: u64) {
        if id > self.manifest_id {
            self.tail.prune(manifest.wal_applied, now_ms);
            self.manifest_id = id;
            self.manifest = manifest;
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            tail: self.tail.mem().clone(),
            tree: self.manifest.tree,
        }
    }
}

/// Manifests probed one id at a time before a LIST finds the newest
/// directly: a reader that fell behind catches up in one step.
const PROBES: u64 = 4;

/// The newest manifest after `current`, if any.
pub async fn newer_manifest(
    store: &Store,
    current: ManifestId,
) -> Result<Option<(ManifestId, Manifest)>> {
    let mut newest = None;
    let mut id = current.next();
    while let Some(m) = manifest::load(store, id).await? {
        newest = Some((id, m));
        if id.0 - current.0 >= PROBES {
            return Ok(match manifest::load_latest(store).await? {
                Some((latest, m)) if latest > id => Some((latest, m)),
                _ => newest,
            });
        }
        id = id.next();
    }
    Ok(newest)
}

/// Every slot from `scan.next()` up to the first empty one.
pub async fn read_log(store: &Store, mut scan: Scan) -> Result<LogRead> {
    loop {
        let record = wal::read(store, scan.next()).await?;
        scan = match scan.record(record) {
            Ok(read) => return Ok(read),
            Err(more) => more,
        };
    }
}

pub struct Follower {
    nodes: Arc<Nodes>,
    rewarm: Warm,
    view: RwLock<View>,
    refreshing: tokio::sync::Mutex<()>,
}

impl Follower {
    /// `warm` loads the tree's internal nodes now; `rewarm` does it again
    /// for every new manifest.
    pub async fn open(nodes: Arc<Nodes>, warm: Warm, rewarm: Warm) -> Result<Self> {
        // An empty database reads as the initial manifest under id 0, which
        // any published manifest supersedes.
        let (manifest_id, manifest) = manifest::load_latest(nodes.store())
            .await?
            .unwrap_or_else(|| (ManifestId(0), Manifest::initial(0)));
        nodes.warm(manifest.tree, warm).await?;
        Ok(Self {
            nodes,
            rewarm,
            view: RwLock::new(View::new(manifest_id, manifest)),
            refreshing: tokio::sync::Mutex::new(()),
        })
    }

    pub fn view(&self) -> View {
        self.view.read().clone()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.view.read().snapshot()
    }

    /// Pulls a newer manifest and the log past it. Returns whether anything
    /// changed.
    pub async fn refresh(&self, now_ms: u64) -> Result<bool> {
        let _guard = self.refreshing.lock().await;
        let store = self.nodes.store();
        let current = self.view.read().manifest_id;
        let newer = newer_manifest(store, current).await?;
        if let Some((_, m)) = &newer {
            self.nodes.warm(m.tree, self.rewarm).await?;
        }
        let installed = newer.is_some();
        let from = {
            let mut v = self.view.write();
            if let Some((id, m)) = newer {
                v.install(id, Arc::new(m), now_ms);
            }
            v.tail.next()
        };
        let read = read_log(store, Scan::from(from)).await?;
        let changed = installed || !read.batches.is_empty() || !read.fences.is_empty();
        self.view.write().tail.absorb(read, now_ms);
        Ok(changed)
    }
}
