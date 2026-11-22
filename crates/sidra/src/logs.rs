//! Where writers' logs live.
//!
//! Everything shared (manifests, segments, leases) lives in the home store. A
//! writer's log can live in another store instead, typically a bucket in the
//! writer's own region, so writes stay local. Its lease records which one, and
//! every process that pulls logs (the applier, readers, GC) needs a store
//! configured under that region name.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::hlc::WriterId;
use crate::location;
use crate::store::Store;
use crate::wal::{self, Lease};

/// A configured name for a log store, such as `eu-west-1`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Region(String);

impl Region {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where one writer's log lives.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogHome {
    /// With everything else, in the home store.
    #[default]
    Home,
    /// In the store configured under this region name.
    Region(Region),
}

/// Log stores by region, built from URLs in settings plus stores passed in
/// code (which win).
#[derive(Clone, Default)]
pub struct RegionStores {
    stores: BTreeMap<Region, (Arc<dyn ObjectStore>, Path)>,
}

impl RegionStores {
    pub fn from_urls(urls: &BTreeMap<Region, String>) -> Result<Self> {
        urls.iter()
            .map(|(r, url)| Ok((r.clone(), location::parse(url)?)))
            .collect::<Result<_>>()
            .map(|stores| Self { stores })
    }

    pub fn insert(&mut self, region: Region, store: Arc<dyn ObjectStore>, path: Path) {
        self.stores.insert(region, (store, path));
    }

    /// Adds `other`'s stores, replacing any for the same region.
    pub fn extend(&mut self, other: RegionStores) {
        self.stores.extend(other.stores);
    }
}

/// Resolves writers to the store holding their log.
pub struct Logs {
    home: Store,
    regions: BTreeMap<Region, Store>,
    /// Placement never changes for a writer id, so it's learned once.
    placed: RwLock<BTreeMap<WriterId, LogHome>>,
}

impl Logs {
    pub fn new(home: Store, regions: RegionStores) -> Self {
        let regions = regions
            .stores
            .into_iter()
            .map(|(r, (os, path))| (r, home.sibling(os, path)))
            .collect();
        Self {
            home,
            regions,
            placed: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn home(&self) -> &Store {
        &self.home
    }

    pub fn at(&self, at: &LogHome) -> Result<&Store> {
        match at {
            LogHome::Home => Ok(&self.home),
            LogHome::Region(r) => self.regions.get(r).ok_or_else(|| {
                Error::InvalidConfig(format!("no store configured for log region {r}"))
            }),
        }
    }

    /// Records where a lease says its writer's log lives.
    pub fn learn(&self, lease: &Lease) {
        self.placed
            .write()
            .entry(lease.writer)
            .or_insert_with(|| lease.log.clone());
    }

    /// The store holding `writer`'s log. A writer writes its lease before
    /// its first log entry, so a writer without a lease has no log entries,
    /// and its home store reads as empty.
    pub async fn of(&self, writer: WriterId) -> Result<&Store> {
        let known = self.placed.read().get(&writer).cloned();
        let at = match known {
            Some(at) => at,
            None => match wal::read_lease(&self.home, writer).await? {
                Some(lease) => {
                    self.learn(&lease);
                    lease.log
                }
                None => LogHome::Home,
            },
        };
        self.at(&at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_home_reads_from_toml_shapes() {
        use figment::Figment;
        use figment::providers::{Format, Toml};
        #[derive(Deserialize)]
        struct T {
            a: LogHome,
            b: LogHome,
        }
        let t: T = Figment::from(Toml::string("a = \"home\"\nb = { region = \"eu\" }"))
            .extract()
            .unwrap();
        assert_eq!(t.a, LogHome::Home);
        assert_eq!(t.b, LogHome::Region(Region::new("eu")));
    }
}
