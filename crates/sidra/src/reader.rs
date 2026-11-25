//! Read-only handle: the latest published tree plus the log past it,
//! refreshed periodically. Must refresh more often than the GC retention, or
//! segments of its tree version may be deleted under it.

use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio::task::JoinHandle;

use crate::db::{Clock, SystemClock};
use crate::error::Result;
use crate::follow::Follower;
use crate::location;
use crate::nodes::Nodes;
use crate::settings::ReaderSettings;
use crate::snapshot::DbIterator;
use crate::store::Store;
use crate::types::{KeyRange, ManifestId};

/// How current a read must be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Freshness {
    /// What this reader has seen: the writer's writes within a refresh
    /// interval.
    #[default]
    Local,
    /// First pull the newest manifest and the log past it (a few GETs), so
    /// the read sees every write acknowledged before it started.
    Latest,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReadOptions {
    pub freshness: Freshness,
}

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
    origin: Option<(Arc<dyn ObjectStore>, Path)>,
}

impl DbReaderBuilder {
    pub fn with_settings(mut self, settings: ReaderSettings) -> Self {
        self.settings = settings;
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
        let origin = match (self.origin, &self.settings.origin) {
            (Some(o), _) => Some(o),
            (None, Some(url)) => Some(location::parse(url)?),
            (None, None) => None,
        };
        DbReader::start(self.store, self.path, self.settings, origin).await
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
            origin: None,
        }
    }

    async fn start(
        os: Arc<dyn ObjectStore>,
        path: Path,
        cfg: ReaderSettings,
        origin: Option<(Arc<dyn ObjectStore>, Path)>,
    ) -> Result<Self> {
        let home = Store::new(os, path, cfg.hedge);
        let nodes = Arc::new(match origin {
            Some((os, path)) => {
                let origin = home.sibling(os, path);
                Nodes::replica(home, origin, cfg.cache)
            }
            None => Nodes::new(home, cfg.cache),
        });
        let follower = Follower::open(nodes.clone(), cfg.warm, cfg.rewarm).await?;
        let shared = Arc::new(Shared {
            nodes,
            follower,
            clock: Arc::new(SystemClock),
        });
        shared.follower.refresh(shared.clock.now_ms()).await?;
        let task = cfg.refresh_interval.map(|every| {
            let shared = shared.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    if let Err(e) = shared.follower.refresh(shared.clock.now_ms()).await {
                        tracing::warn!(%e, "reader refresh failed");
                    }
                }
            })
        });
        Ok(Self { shared, task })
    }

    /// Pulls the newest manifest and the log past it now.
    pub async fn refresh(&self) -> Result<()> {
        self.shared
            .follower
            .refresh(self.shared.clock.now_ms())
            .await
            .map(drop)
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
