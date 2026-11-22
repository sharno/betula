//! Settings for writers and readers. Every field has a default, so a config
//! file or the environment only needs to name what it changes.
//!
//! Sources, later ones winning: defaults, then a TOML file (`SIDRA_CONFIG`
//! or an explicit path), then `SIDRA_*` environment variables, with `__`
//! separating sections:
//!
//! ```toml
//! [wal]
//! flush_interval = "20ms"
//!
//! [tree]
//! leaf_max_bytes = 262144
//! ```
//!
//! ```sh
//! SIDRA_WAL__FLUSH_INTERVAL=20ms SIDRA_APPLIER__MODE=never
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

use crate::codec::Compression;
use crate::error::{Error, Result};
use crate::gc::GcConfig;
use crate::logs::{LogHome, Region};
use crate::nodes::{CacheConfig, Warm};
use crate::segments::CleanerConfig;
use crate::tree::TreeConfig;

const ENV_PREFIX: &str = "SIDRA_";
const CONFIG_FILE_VAR: &str = "SIDRA_CONFIG";

/// Writer settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub wal: WalConfig,
    pub checkpoint: CheckpointConfig,
    pub poll: PollConfig,
    pub lease: LeaseConfig,
    pub applier: ApplierRole,
    pub gc: Gc,
    pub tree: TreeConfig,
    pub cache: CacheConfig,
    pub cleaner: CleanerConfig,
    /// Compression of tree nodes and log batches. Readers decode either.
    pub compression: Compression,
    /// Where this writer's log goes. A writer far from the home store puts
    /// it in a store in its own region (listed in `regions`), so writes stay
    /// local; the applier pulls it from there.
    pub log: LogHome,
    /// Stores holding writers' logs, by region name: URLs such as
    /// `s3://bucket-eu/db`. Every process that pulls logs (applier, readers,
    /// GC) needs each region that any writer logs to.
    pub regions: BTreeMap<Region, String>,
    /// Loading of internal nodes and buffered runs on open.
    pub warm: Warm,
    /// Loading of each new manifest's changed internal nodes and runs as
    /// soon as it is seen. Every writer doing this costs GETs proportional to
    /// writers × checkpoint size, so writers default to loading on first use.
    pub rewarm: Warm,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            wal: WalConfig::default(),
            checkpoint: CheckpointConfig::default(),
            poll: PollConfig::default(),
            lease: LeaseConfig::default(),
            applier: ApplierRole::default(),
            gc: Gc::On(GcConfig::default()),
            tree: TreeConfig::default(),
            cache: CacheConfig::default(),
            cleaner: CleanerConfig::default(),
            compression: Compression::default(),
            log: LogHome::Home,
            regions: BTreeMap::new(),
            warm: Warm::Blocking,
            rewarm: Warm::Off,
        }
    }
}

/// Reader settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ReaderSettings {
    /// Background refresh period; `None` (omitted) means only on demand.
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Option<Duration>,
    /// How often background refreshes also LIST for new writers.
    #[serde(with = "humantime_serde")]
    pub discovery_interval: Duration,
    pub cache: CacheConfig,
    pub warm: Warm,
    pub rewarm: Warm,
    /// Stores holding writers' logs, by region name (see `Settings`).
    pub regions: BTreeMap<Region, String>,
    /// When the reader opens a replica of the database (bucket replication
    /// into its own region), the URL of the original. Segments that haven't
    /// replicated yet are read from it.
    pub origin: Option<String>,
}

impl Default for ReaderSettings {
    fn default() -> Self {
        Self {
            refresh_interval: Some(Duration::from_millis(500)),
            discovery_interval: Duration::from_secs(2),
            cache: CacheConfig::default(),
            warm: Warm::Blocking,
            rewarm: Warm::Background,
            regions: BTreeMap::new(),
            origin: None,
        }
    }
}

macro_rules! loaders {
    ($ty:ty) => {
        impl $ty {
            /// Defaults, then the file named by `SIDRA_CONFIG` if set, then
            /// `SIDRA_*` environment variables.
            pub fn load() -> Result<Self> {
                let figment = Figment::from(Serialized::defaults(Self::default()));
                let figment = match std::env::var_os(CONFIG_FILE_VAR) {
                    Some(path) => figment.merge(Toml::file_exact(path)),
                    None => figment,
                };
                extract(figment.merge(Env::prefixed(ENV_PREFIX).split("__")))
            }

            /// Defaults, then `path`, then `SIDRA_*` environment variables.
            pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
                extract(
                    Figment::from(Serialized::defaults(Self::default()))
                        .merge(Toml::file_exact(path.as_ref()))
                        .merge(Env::prefixed(ENV_PREFIX).split("__")),
                )
            }

            /// Defaults overridden by TOML text.
            pub fn from_toml(text: &str) -> Result<Self> {
                extract(
                    Figment::from(Serialized::defaults(Self::default())).merge(Toml::string(text)),
                )
            }
        }
    };
}

loaders!(Settings);
loaders!(ReaderSettings);

fn extract<T: serde::de::DeserializeOwned>(figment: Figment) -> Result<T> {
    figment
        .extract()
        .map_err(|e| Error::InvalidConfig(e.to_string()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// A write is acknowledged once the log batch holding it is durable;
    /// batches are written this often...
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// ...or once this many bytes are pending.
    pub max_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_millis(50),
            max_bytes: 4 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointConfig {
    /// The applier checkpoints once unapplied entries reach this size...
    pub bytes: usize,
    /// ...or once the oldest has waited this long.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Writers stall while unapplied entries exceed this.
    pub max_unapplied_bytes: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            bytes: 32 << 20,
            interval: Duration::from_secs(2),
            max_unapplied_bytes: 512 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct PollConfig {
    /// How often to poll for other writers' entries and new manifests while
    /// they are arriving.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Polling backs off up to this while nothing arrives, so an idle
    /// database costs few requests. The first write after a quiet period
    /// becomes visible to other writers within this.
    #[serde(with = "humantime_serde")]
    pub max_interval: Duration,
    /// How often to LIST for new writers.
    #[serde(with = "humantime_serde")]
    pub discovery_interval: Duration,
    pub follow: Follow,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(200),
            max_interval: Duration::from_secs(2),
            discovery_interval: Duration::from_secs(2),
            follow: Follow::AllWriters,
        }
    }
}

/// Whose unapplied writes a writer pulls in the background.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Follow {
    /// Every writer's: other writers' writes show up within a poll. Costs
    /// about (writers - 1) x log bytes in GETs per writer.
    #[default]
    AllWriters,
    /// Only the manifest: other writers' writes show up once applied (about
    /// a checkpoint interval). Reads with `Freshness::Latest` still pull
    /// everything. The applier always pulls every writer.
    ManifestOnly,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LeaseConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Must comfortably exceed `interval` plus clock skew between processes.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            ttl: Duration::from_secs(20),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ApplierRole {
    /// Take the role when the database has no applier, or when entries have
    /// sat unapplied longer than `steal_after`.
    Auto {
        #[serde(with = "humantime_serde")]
        steal_after: Duration,
    },
    /// Take the role only when work has sat unapplied for `after` and no
    /// manifest was published meanwhile (no applier, or a stuck or dead
    /// one), and hand it back once a live `auto` writer exists. For writers
    /// far from the home store: they apply only while the home region can't.
    Standby {
        #[serde(with = "humantime_serde")]
        after: Duration,
    },
    /// Only write; leave applying to other processes.
    Never,
}

impl ApplierRole {
    pub(crate) fn candidacy(&self) -> Candidacy {
        match self {
            ApplierRole::Auto { .. } => Candidacy::Primary,
            ApplierRole::Standby { .. } => Candidacy::Standby,
            ApplierRole::Never => Candidacy::Never,
        }
    }
}

/// How a writer competes for the applier role, as its lease advertises.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Candidacy {
    Primary,
    Standby,
    Never,
}

impl Default for ApplierRole {
    fn default() -> Self {
        ApplierRole::Auto {
            steal_after: Duration::from_secs(10),
        }
    }
}

/// Whether the applier runs garbage collection.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Gc {
    Off,
    On(GcConfig),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toml_overrides_only_what_it_names() {
        let s = Settings::from_toml(
            r#"
            warm = "background"
            [wal]
            flush_interval = "20ms"
            [applier]
            mode = "never"
            [gc]
            mode = "off"
            "#,
        )
        .unwrap();
        assert_eq!(s.wal.flush_interval, Duration::from_millis(20));
        assert_eq!(s.wal.max_bytes, WalConfig::default().max_bytes);
        assert!(matches!(s.applier, ApplierRole::Never));
        assert!(matches!(s.gc, Gc::Off));
        assert_eq!(s.warm, Warm::Background);
        assert_eq!(s.tree.max_fanout, TreeConfig::default().max_fanout);
    }

    #[test]
    fn bad_values_are_config_errors() {
        let e = Settings::from_toml("[wal]\nflush_interval = \"soon\"").unwrap_err();
        assert!(matches!(e, Error::InvalidConfig(_)), "{e}");
    }
}
