//! Inspection and maintenance of a database without opening a writer.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::db::{Clock, SystemClock};
use crate::error::Result;
use crate::gc::{self, GcStats};
use crate::hlc::WriterId;
use crate::location;
use crate::manifest;
use crate::nodes::{CacheConfig, Nodes};
use crate::store::{Pace, Store};
use crate::tree::{self, TreeShape};
use crate::types::{Epoch, ManifestId, TreeRoot};
use crate::wal;

/// The current published version.
#[derive(Clone, Debug)]
pub struct ManifestInfo {
    pub id: ManifestId,
    pub applier_epoch: Epoch,
    /// The writer holding the applier role, if any.
    pub applier: Option<WriterId>,
    pub tree: Option<TreeRoot>,
    pub created_at_ms: u64,
    pub segments: usize,
    pub segment_bytes: u64,
    pub live_bytes: u64,
    /// Last applied log sequence number per writer.
    pub applied: BTreeMap<WriterId, u64>,
}

#[derive(Clone, Debug)]
pub struct WriterInfo {
    pub id: WriterId,
    /// Lease expiry by the writer's clock; `None` if it never wrote one.
    pub lease_expires_at_ms: Option<u64>,
    pub live: bool,
    /// Log objects still in storage, and how many of them aren't applied.
    pub log_objects: usize,
    pub unapplied: usize,
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
        Self {
            nodes: Nodes::new(Store::new(store, path), cache),
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
                applier_epoch: m.applier_epoch,
                applier: m.applier,
                tree: m.tree,
                created_at_ms: m.created_at_ms,
                segments: m.segments.len(),
                segment_bytes: m.segments.values().map(|u| u.total).sum(),
                live_bytes: m.segments.values().map(|u| u.live).sum(),
                applied: m.frontier,
            }))
    }

    pub async fn writers(&self) -> Result<Vec<WriterInfo>> {
        let store = self.store();
        let applied = self
            .manifest()
            .await?
            .map(|m| m.applied)
            .unwrap_or_default();
        let now = self.clock.now_ms();
        let ids = store.list_writers().await?;
        try_join_all(ids.into_iter().map(|id| {
            let upto = applied.get(&id).copied().unwrap_or(0);
            async move {
                let (lease, seqs) =
                    futures::try_join!(wal::read_lease(store, id), store.list_wals(id))?;
                Ok(WriterInfo {
                    id,
                    lease_expires_at_ms: lease.map(|l| l.expires_at_ms),
                    live: lease.is_some_and(|l| l.expires_at_ms > now),
                    log_objects: seqs.len(),
                    unapplied: seqs.iter().filter(|s| **s > upto).count(),
                })
            }
        }))
        .await
    }

    /// Walks the current tree. Loads every node, so it costs a full read.
    pub async fn tree_shape(&self) -> Result<TreeShape> {
        let root = self.manifest().await?.and_then(|m| m.tree);
        tree::shape(&self.nodes, root).await
    }

    /// Deletes what no reader within `retention` can need. Safe to run while
    /// writers are active.
    pub async fn gc(&self, retention: Duration) -> Result<GcStats> {
        gc::run(
            self.store(),
            self.clock.now_ms(),
            retention,
            Pace::Unlimited,
        )
        .await
    }
}
