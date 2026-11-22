//! Read-only handle: the latest published tree plus every writer's unapplied
//! log tail, refreshed periodically. Must refresh more often than the GC
//! retention, or segments of its tree version may be deleted under it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::task::JoinHandle;

use crate::db::{Clock, SystemClock};
use crate::error::Result;
use crate::follow::Follower;
use crate::nodes::{CacheConfig, Nodes, Warm};
use crate::snapshot::DbIterator;
use crate::store::Store;
use crate::types::{KeyRange, ManifestId};

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub cache: CacheConfig,
    /// Background refresh period; `None` means refresh only on demand.
    pub refresh_interval: Option<Duration>,
    /// How often background refreshes also LIST for new writers.
    pub discovery_interval: Duration,
    /// Loading internal nodes and buffered runs on open.
    pub warm: Warm,
    /// Loading of each new manifest's internal nodes as soon as it is seen.
    pub rewarm: Warm,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig::default(),
            refresh_interval: Some(Duration::from_millis(500)),
            discovery_interval: Duration::from_secs(2),
            warm: Warm::Blocking,
            rewarm: Warm::Background,
        }
    }
}

struct Shared {
    nodes: Arc<Nodes>,
    follower: Follower,
    clock: Arc<dyn Clock>,
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
        let nodes = Arc::new(Nodes::new(Store::new(os, path), cfg.cache));
        let follower = Follower::open(nodes.clone(), cfg.warm, cfg.rewarm, None).await?;
        let shared = Arc::new(Shared {
            nodes,
            follower,
            clock: Arc::new(SystemClock),
        });
        shared.follower.refresh(true, shared.clock.now_ms()).await?;
        let task = cfg.refresh_interval.map(|every| {
            let shared = shared.clone();
            tokio::spawn(async move {
                let mut last_discovery = Instant::now();
                loop {
                    tokio::time::sleep(every).await;
                    let discover = last_discovery.elapsed() >= cfg.discovery_interval;
                    if discover {
                        last_discovery = Instant::now();
                    }
                    if let Err(e) = shared
                        .follower
                        .refresh(discover, shared.clock.now_ms())
                        .await
                    {
                        tracing::warn!(%e, "reader refresh failed");
                    }
                }
            })
        });
        Ok(Self { shared, task })
    }

    pub async fn refresh(&self) -> Result<()> {
        self.shared
            .follower
            .refresh(true, self.shared.clock.now_ms())
            .await
            .map(|_| ())
    }

    pub fn manifest_id(&self) -> ManifestId {
        self.shared.follower.view().manifest_id
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let version = self
            .shared
            .follower
            .snapshot()
            .get(&self.shared.nodes, key)
            .await?;
        Ok(version.and_then(|v| v.value().cloned()))
    }

    pub fn scan(&self, range: impl Into<KeyRange>) -> DbIterator {
        self.shared
            .follower
            .snapshot()
            .scan(self.shared.nodes.clone(), range.into())
    }
}

impl Drop for DbReader {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}
