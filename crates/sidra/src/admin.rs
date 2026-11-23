//! Inspection and maintenance of a database without opening a writer.

use std::collections::{BTreeMap, BTreeSet};
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
use crate::logs::{LogHome, Logs, Region, RegionStores};
use crate::manifest;
use crate::nodes::{CacheConfig, Nodes};
use crate::settings::Settings;
use crate::store::Store;
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
    /// Writers whose log is sealed right after their applied entries.
    pub sealed: BTreeSet<WriterId>,
}

#[derive(Clone, Debug)]
pub struct WriterInfo {
    pub id: WriterId,
    /// Where its log lives; `None` if it never wrote a lease.
    pub log: Option<LogHome>,
    /// Lease expiry by the writer's clock; `None` if it never wrote one.
    pub lease_expires_at_ms: Option<u64>,
    pub live: bool,
    /// Its log is sealed and fully applied: it can't write anymore.
    pub sealed: bool,
    /// Log objects still in storage (a seal included), and how many entries
    /// aren't applied.
    pub log_objects: usize,
    pub unapplied: usize,
}

pub struct Admin {
    nodes: Nodes,
    regions: RegionStores,
    logs: Logs,
    clock: Arc<dyn Clock>,
}

impl Admin {
    /// Log regions come from [`Settings::load`].
    pub fn open(url: &str) -> Result<Self> {
        let (store, path) = location::parse(url)?;
        let regions = RegionStores::from_urls(&Settings::load()?.regions)?;
        Ok(Self::with_regions(path, store, regions))
    }

    pub fn new(path: impl Into<Path>, store: Arc<dyn ObjectStore>) -> Self {
        Self::with_regions(path, store, RegionStores::default())
    }

    fn with_regions(
        path: impl Into<Path>,
        store: Arc<dyn ObjectStore>,
        regions: RegionStores,
    ) -> Self {
        let cache = CacheConfig {
            inner_bytes: 64 << 20,
            leaf_bytes: 16 << 20,
        };
        let home = Store::new(store, path);
        Self {
            logs: Logs::new(home.clone(), regions.clone()),
            nodes: Nodes::new(home, cache),
            regions,
            clock: Arc::new(SystemClock),
        }
    }

    /// Adds a store holding writers' logs (see [`Settings::regions`]).
    pub fn with_region(
        self,
        region: Region,
        store: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
    ) -> Self {
        let mut regions = self.regions;
        regions.insert(region, store, path.into());
        let home = self.nodes.store().clone();
        Self {
            logs: Logs::new(home, regions.clone()),
            regions,
            ..self
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
                sealed: m.sealed,
            }))
    }

    pub async fn writers(&self) -> Result<Vec<WriterInfo>> {
        let store = self.store();
        let (applied, sealed) = self
            .manifest()
            .await?
            .map(|m| (m.applied, m.sealed))
            .unwrap_or_default();
        let now = self.clock.now_ms();
        let ids = store.list_writers().await?;
        try_join_all(ids.into_iter().map(|id| {
            let upto = applied.get(&id).copied().unwrap_or(0);
            let sealed = sealed.contains(&id);
            async move {
                let (lease, seqs) = futures::try_join!(wal::read_lease(store, id), async {
                    self.logs.of(id).await?.list_wals(id).await
                })?;
                Ok(WriterInfo {
                    id,
                    log: lease.as_ref().map(|l| l.log.clone()),
                    lease_expires_at_ms: lease.as_ref().map(|l| l.expires_at_ms),
                    live: !sealed && lease.is_some_and(|l| l.expires_at_ms > now),
                    sealed,
                    log_objects: seqs.len(),
                    // A sealed log's only object past `upto` is the seal.
                    unapplied: seqs
                        .iter()
                        .filter(|s| **s > upto + u64::from(sealed))
                        .count(),
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
        gc::run(&self.logs, self.clock.now_ms(), retention, gc::Speed::Full).await
    }
}
