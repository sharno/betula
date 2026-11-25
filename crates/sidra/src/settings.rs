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
//! SIDRA_WAL__FLUSH_INTERVAL=20ms SIDRA_GC__MODE=off
//! ```

use std::path::Path;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

use crate::codec::Compression;
use crate::error::{Error, Result};
use crate::gc::GcConfig;
use crate::hedge::Hedge;
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
    pub gc: Gc,
    pub tree: TreeConfig,
    pub cache: CacheConfig,
    pub cleaner: CleanerConfig,
    /// Compression of tree nodes and log batches. Readers decode either.
    pub compression: Compression,
    /// Loading of internal nodes and buffered runs on open.
    pub warm: Warm,
    /// Second copies of GETs slower than the recent p95.
    pub hedge: Hedge,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            wal: WalConfig::default(),
            checkpoint: CheckpointConfig::default(),
            gc: Gc::On(GcConfig::default()),
            tree: TreeConfig::default(),
            cache: CacheConfig::default(),
            cleaner: CleanerConfig::default(),
            compression: Compression::default(),
            warm: Warm::Blocking,
            hedge: Hedge::default(),
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
    pub cache: CacheConfig,
    pub warm: Warm,
    pub rewarm: Warm,
    /// When the reader opens a replica of the database (bucket replication
    /// into its own region), the URL of the original. Segments that haven't
    /// replicated yet are read from it.
    pub origin: Option<String>,
    /// Second copies of GETs slower than the recent p95.
    pub hedge: Hedge,
}

impl Default for ReaderSettings {
    fn default() -> Self {
        Self {
            refresh_interval: Some(Duration::from_millis(500)),
            cache: CacheConfig::default(),
            warm: Warm::Blocking,
            rewarm: Warm::Background,
            origin: None,
            hedge: Hedge::default(),
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
    /// The writer checkpoints once unapplied writes reach this size...
    pub bytes: usize,
    /// ...or once the oldest has waited this long. Also how often an idle
    /// writer checks whether a newer one fenced it.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// A checkpoint applies at most this much, oldest writes first, so it
    /// takes about as long however far behind the writer is, and frees
    /// backlog steadily.
    pub max_bytes: usize,
    /// Writes stall while unapplied ones reach this.
    pub max_unapplied_bytes: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            bytes: 32 << 20,
            interval: Duration::from_secs(2),
            max_bytes: 64 << 20,
            max_unapplied_bytes: 512 << 20,
        }
    }
}

/// Whether the writer runs garbage collection.
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
            [gc]
            mode = "off"
            "#,
        )
        .unwrap();
        assert_eq!(s.wal.flush_interval, Duration::from_millis(20));
        assert_eq!(s.wal.max_bytes, WalConfig::default().max_bytes);
        assert!(matches!(s.gc, Gc::Off));
        assert_eq!(s.warm, Warm::Background);
        assert_eq!(s.tree.max_fanout, TreeConfig::default().max_fanout);
    }

    #[test]
    fn hedge_percentile_outside_0_to_1_is_rejected() {
        let toml = "[hedge]\nmode = \"on\"\npercentile = 1.5";
        assert!(Settings::from_toml(toml).is_err());
        let toml = "[hedge]\nmode = \"on\"\npercentile = 0.99";
        assert!(Settings::from_toml(toml).is_ok());
    }

    #[test]
    fn bad_values_are_config_errors() {
        let e = Settings::from_toml("[wal]\nflush_interval = \"soon\"").unwrap_err();
        assert!(matches!(e, Error::InvalidConfig(_)), "{e}");
    }
}
