//! Inspection and maintenance of a database without opening a writer.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::path::Path;

use crate::db::{Clock, SystemClock};
use crate::error::Result;
use crate::gc::{self, GcStats};
use crate::location;
use crate::manifest;
use crate::nodes::{CacheConfig, Nodes};
use crate::store::Store;
use crate::tree::{self, TreeShape};
use crate::types::{Epoch, ManifestId, TreeRoot, WalSeq};

/// The current published version.
#[derive(Clone, Debug)]
pub struct ManifestInfo {
    pub id: ManifestId,
    /// The newest writer generation to open the database.
    pub writer_epoch: Epoch,
    pub tree: Option<TreeRoot>,
    pub created_at_ms: u64,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
    /// Log slots up to here are in the tree.
    pub wal_applied: WalSeq,
}

/// The log objects in storage.
#[derive(Clone, Debug)]
pub struct LogInfo {
    /// Slots still stored, applied ones included (GC deletes them after
    /// the retention), and fences kept for good.
    pub objects: usize,
    /// Slots past the current manifest's applied one.
    pub unapplied: usize,
    pub fences: usize,
}

pub struct Admin {
    nodes: Nodes,
    clock: Arc<dyn Clock>,
}

impl Admin {
    pub fn open(url: &str) -> Result<Self> {
        let (store, path) = location::parse(url)?;
        Ok(Self::new(path, store))
    }

    pub fn new(path: impl Into<Path>, store: Arc<dyn ObjectStore>) -> Self {
        let cache = CacheConfig {
            inner_bytes: 64 << 20,
            leaf_bytes: 16 << 20,
        };
        let home = Store::new(store, path, crate::Hedge::default());
        Self {
            nodes: Nodes::new(home, cache),
            clock: Arc::new(SystemClock),
        }
    }

    fn store(&self) -> &Store {
        self.nodes.store()
    }

    /// `None` for an empty (never opened) database.
    pub async fn manifest(&self) -> Result<Option<ManifestInfo>> {
        Ok(manifest::load_latest(self.store())
            .await?
            .map(|(id, m)| ManifestInfo {
                id,
                writer_epoch: m.writer_epoch,
                tree: m.tree,
                created_at_ms: m.created_at_ms,
                segments: m.segments.len(),
                segment_bytes: m.segments.values().map(|u| u.total).sum(),
                live_bytes: m.segments.values().map(|u| u.live).sum(),
                wal_applied: m.wal_applied,
            }))
    }

    pub async fn log(&self) -> Result<LogInfo> {
        let store = self.store();
        let (latest, seqs) = futures::try_join!(manifest::load_latest(store), store.list_wals())?;
        let (applied, fences) = latest
            .map(|(_, m)| (m.wal_applied, m.fences.len()))
            .unwrap_or((WalSeq::ZERO, 0));
        Ok(LogInfo {
            objects: seqs.len(),
            unapplied: seqs.iter().filter(|s| **s > applied).count(),
            fences,
        })
    }

    /// Walks the current tree. Loads every node, so it costs a full read.
    pub async fn tree_shape(&self) -> Result<TreeShape> {
        let root = self.manifest().await?.and_then(|m| m.tree);
        tree::shape(&self.nodes, root).await
    }

    /// Deletes what no reader within `retention` can need. Safe to run while
    /// a writer is active.
    pub async fn gc(&self, retention: Duration) -> Result<GcStats> {
        gc::run(
            self.store(),
            self.clock.now_ms(),
            retention,
            gc::Speed::Full,
        )
        .await
    }
}
