//! Read-only handle: the latest published tree plus every writer's unapplied
//! log tail, refreshed periodically. Must refresh more often than the GC
//! retention, or segments of its tree version may be deleted under it.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::task::JoinHandle;

use crate::db::{Clock, Freshness, ReadOptions, SystemClock};
use crate::error::Result;
use crate::follow::{Follower, Scope};
use crate::location;
use crate::logs::{Logs, Region, RegionStores};
use crate::nodes::Nodes;
use crate::settings::ReaderSettings;
use crate::snapshot::DbIterator;
use crate::store::Store;
use crate::types::{KeyRange, ManifestId};

struct Shared {
    nodes: Arc<Nodes>,
    follower: Follower,
    clock: Arc<dyn Clock>,
}

/// Configures and opens a reader. See [`DbReader::builder`].
pub struct DbReaderBuilder {
    store: Arc<dyn ObjectStore>,
    path: Path,
    settings: ReaderSettings,
    regions: RegionStores,
    origin: Option<(Arc<dyn ObjectStore>, Path)>,
}

impl DbReaderBuilder {
    pub fn with_settings(mut self, settings: ReaderSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Adds a store holding writers' logs, overriding a URL for the same
    /// region in settings.
    pub fn with_region(
        mut self,
        region: Region,
        store: Arc<dyn ObjectStore>,
        path: impl Into<Path>,
    ) -> Self {
        self.regions.insert(region, store, path.into());
        self
    }

    /// Reads a replica: `store` is a bucket-replicated copy of `origin`, and
    /// segments that haven't replicated yet are read from `origin`.
    /// Overrides `origin` in settings.
    pub fn with_origin(mut self, origin: Arc<dyn ObjectStore>, path: impl Into<Path>) -> Self {
        self.origin = Some((origin, path.into()));
        self
    }

    pub async fn build(self) -> Result<DbReader> {
        let mut regions = RegionStores::from_urls(&self.settings.regions)?;
        regions.extend(self.regions);
        let origin = match (self.origin, &self.settings.origin) {
            (Some(o), _) => Some(o),
            (None, Some(url)) => Some(location::parse(url)?),
            (None, None) => None,
        };
        DbReader::start(self.store, self.path, self.settings, regions, origin).await
    }
}

pub struct DbReader {
    shared: Arc<Shared>,
    task: Option<JoinHandle<()>>,
}

impl DbReader {
    /// Opens a reader at a URL (see [`crate::Db::open`]), with settings from
    /// [`ReaderSettings::load`].
    pub async fn open(url: &str) -> Result<Self> {
        let (store, path) = location::parse(url)?;
        Self::builder(path, store)
            .with_settings(ReaderSettings::load()?)
            .build()
            .await
    }

    pub fn builder(path: impl Into<Path>, store: Arc<dyn ObjectStore>) -> DbReaderBuilder {
        DbReaderBuilder {
            store,
            path: path.into(),
            settings: ReaderSettings::default(),
            regions: RegionStores::default(),
            origin: None,
        }
    }

    async fn start(
        os: Arc<dyn ObjectStore>,
        path: Path,
        cfg: ReaderSettings,
        regions: RegionStores,
        origin: Option<(Arc<dyn ObjectStore>, Path)>,
    ) -> Result<Self> {
        let home = Store::new(os, path, cfg.hedge);
        let logs = Arc::new(Logs::new(home.clone(), regions));
        let nodes = Arc::new(match origin {
            Some((os, path)) => {
                let origin = home.sibling(os, path);
                Nodes::replica(home, origin, cfg.cache)
            }
            None => Nodes::new(home, cfg.cache),
        });
        let follower = Follower::open(nodes.clone(), logs, cfg.warm, cfg.rewarm, None).await?;
        let shared = Arc::new(Shared {
            nodes,
            follower,
            clock: Arc::new(SystemClock),
        });
        shared
            .follower
            .refresh(true, Scope::AllWriters, shared.clock.now_ms())
            .await?;
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
                        .refresh(discover, Scope::AllWriters, shared.clock.now_ms())
                        .await
                    {
                        tracing::warn!(%e, "reader refresh failed");
                    }
                }
            })
        });
        Ok(Self { shared, task })
    }

    /// Pulls every writer's durable entries and the newest manifest now.
    pub async fn refresh(&self) -> Result<()> {
        self.shared
            .follower
            .refresh(true, Scope::AllWriters, self.shared.clock.now_ms())
            .await?
            .complete()
    }

    pub fn manifest_id(&self) -> ManifestId {
        self.shared.follower.view().manifest_id
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.get_with(key, ReadOptions::default()).await
    }

    pub async fn get_with(
        &self,
        key: impl AsRef<[u8]>,
        opts: ReadOptions,
    ) -> Result<Option<Bytes>> {
        if opts.freshness == Freshness::Latest {
            self.refresh().await?;
        }
        let version = self
            .shared
            .follower
            .snapshot()
            .get(&self.shared.nodes, key.as_ref())
            .await?;
        Ok(version.and_then(|v| v.value().cloned()))
    }

    /// Live pairs in `range`, in key order, from a consistent snapshot.
    pub fn scan<K: AsRef<[u8]>>(&self, range: impl RangeBounds<K>) -> DbIterator {
        self.shared
            .follower
            .snapshot()
            .scan(self.shared.nodes.clone(), KeyRange::from_bounds(range))
    }

    pub async fn scan_with<K: AsRef<[u8]>>(
        &self,
        range: impl RangeBounds<K>,
        opts: ReadOptions,
    ) -> Result<DbIterator> {
        if opts.freshness == Freshness::Latest {
            self.refresh().await?;
        }
        Ok(self.scan(range))
    }
}

impl Drop for DbReader {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}
