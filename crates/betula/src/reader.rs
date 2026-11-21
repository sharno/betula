//! Read-only handle. Serves the latest published tree plus the WAL tail, and
//! refreshes both periodically. Must refresh more often than the writer's GC
//! retention, or nodes of its tree version may be deleted under it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::manifest::{self, Manifest};
use crate::memtable::Memtable;
use crate::nodes::{CacheConfig, Nodes};
use crate::snapshot::{DbIterator, Snapshot};
use crate::store::Store;
use crate::types::{KeyRange, ManifestId, WalId};
use crate::wal::{self, WalBatch};

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub cache: CacheConfig,
    /// Background refresh period; `None` means refresh only on demand.
    pub refresh_interval: Option<Duration>,
    pub warm_inner_nodes: bool,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig::default(),
            refresh_interval: Some(Duration::from_secs(1)),
            warm_inner_nodes: true,
        }
    }
}

struct ReaderState {
    manifest_id: ManifestId,
    manifest: Arc<Manifest>,
    /// WAL batches newer than the manifest, in id order.
    wals: BTreeMap<WalId, Arc<WalBatch>>,
    mem: Memtable,
}

impl ReaderState {
    fn wal_cursor(&self) -> WalId {
        self.wals
            .last_key_value()
            .map_or(self.manifest.wal_replay_after, |(id, _)| *id)
    }
}

struct Shared {
    nodes: Arc<Nodes>,
    cfg: ReaderConfig,
    state: RwLock<ReaderState>,
    /// Serializes refreshes so they apply in order.
    refreshing: tokio::sync::Mutex<()>,
}

pub struct DbReader {
    shared: Arc<Shared>,
    task: Option<JoinHandle<()>>,
}

impl DbReader {
    pub async fn open(
        os: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
        cfg: ReaderConfig,
    ) -> Result<Self> {
        let store = Store::new(os, path);
        // An empty database reads as the initial manifest under id 0, which
        // any published manifest supersedes.
        let (manifest_id, manifest) = manifest::load_latest(&store)
            .await?
            .unwrap_or_else(|| (ManifestId(0), Manifest::initial(0)));
        let nodes = Arc::new(Nodes::new(store, cfg.cache));
        if cfg.warm_inner_nodes {
            nodes.warm_inner(manifest.tree).await?;
        }
        let shared = Arc::new(Shared {
            nodes,
            state: RwLock::new(ReaderState {
                manifest_id,
                manifest: Arc::new(manifest),
                wals: BTreeMap::new(),
                mem: Memtable::default(),
            }),
            refreshing: tokio::sync::Mutex::new(()),
            cfg,
        });
        shared.refresh().await?;
        let task = shared.cfg.refresh_interval.map(|every| {
            let shared = shared.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    if let Err(e) = shared.refresh().await {
                        tracing::warn!(%e, "reader refresh failed");
                    }
                }
            })
        });
        Ok(Self { shared, task })
    }

    pub async fn refresh(&self) -> Result<()> {
        self.shared.refresh().await
    }

    pub fn manifest_id(&self) -> ManifestId {
        self.shared.state.read().manifest_id
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.snapshot().get(&self.shared.nodes, key).await
    }

    pub fn scan(&self, range: impl Into<KeyRange>) -> DbIterator {
        self.snapshot()
            .scan(self.shared.nodes.clone(), range.into())
    }

    fn snapshot(&self) -> Snapshot {
        let s = self.shared.state.read();
        Snapshot {
            mems: vec![s.mem.clone()],
            tree: s.manifest.tree,
        }
    }
}

impl Drop for DbReader {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Shared {
    async fn refresh(&self) -> Result<()> {
        let _guard = self.refreshing.lock().await;
        let store = self.nodes.store();
        let (current_id, cursor) = {
            let s = self.state.read();
            (s.manifest_id, s.wal_cursor())
        };
        let newer = match store.list_manifests().await?.last().copied() {
            Some(id) if id > current_id => {
                let m = manifest::load(store, id)
                    .await?
                    .ok_or_else(|| Error::Corrupt {
                        path: store.manifest_path(id).to_string(),
                        reason: "listed manifest is missing".into(),
                    })?;
                if self.cfg.warm_inner_nodes {
                    self.nodes.warm_inner(m.tree).await?;
                }
                Some((id, Arc::new(m)))
            }
            _ => None,
        };
        let floor = newer
            .as_ref()
            .map_or(cursor, |(_, m)| m.wal_replay_after.max(cursor));
        let fresh = read_wal_tail(store, floor).await?;

        let mut s = self.state.write();
        match newer {
            Some((id, m)) => {
                let keep_after = m.wal_replay_after;
                s.manifest_id = id;
                s.manifest = m;
                s.wals.retain(|wid, _| *wid > keep_after);
                s.wals.extend(fresh);
                // The new tree absorbed part of the tail: rebuild from the rest.
                let mem = s.wals.values().fold(Memtable::default(), |mut mem, b| {
                    mem.apply(b.ops.iter().cloned());
                    mem
                });
                s.mem = mem;
            }
            None => {
                let ops: Vec<_> = fresh
                    .iter()
                    .flat_map(|(_, b)| b.ops.iter().cloned())
                    .collect();
                s.mem.apply(ops);
                s.wals.extend(fresh);
            }
        }
        Ok(())
    }
}

/// Consecutive WAL batches after `after`, stopping at the first gap.
async fn read_wal_tail(store: &Store, after: WalId) -> Result<Vec<(WalId, Arc<WalBatch>)>> {
    let mut out = Vec::new();
    let mut id = after.next();
    while let Some(batch) = wal::read(store, id).await? {
        out.push((id, Arc::new(batch)));
        id = id.next();
    }
    Ok(out)
}
