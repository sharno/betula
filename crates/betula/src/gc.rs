//! Deletes segments, writer log entries and manifests that no reader can
//! need anymore.
//!
//! Manifest `m[i]` lists the segments that died going from `m[i-1]` to `m[i]`.
//! Once `m[i]` is older than the retention, nobody may still be reading
//! `m[i-1]` (readers must refresh within the retention), so `m[i]`'s dead
//! segments, `m[i-1]` itself, and writer log entries already folded into
//! `m[i]` can go. The latest manifest is never deleted.

use std::time::Duration;

use crate::error::Result;
use crate::manifest;
use crate::manifest::Frontier;
use crate::store::Store;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GcConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Minimum age of a superseding manifest before the version it replaced is
    /// deleted. Must exceed the longest snapshot or reader refresh interval.
    #[serde(with = "humantime_serde")]
    pub retention: Duration,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            retention: Duration::from_secs(600),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    pub manifests: usize,
    pub segments: usize,
    pub wals: usize,
}

pub async fn run(store: &Store, now_ms: u64, retention: Duration) -> Result<GcStats> {
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let ids = store.list_manifests().await?;
    let mut stats = GcStats::default();
    let mut applied = Frontier::new();
    for pair in ids.windows(2) {
        let (prev, cur) = (pair[0], pair[1]);
        let Some(m) = manifest::load(store, cur).await? else {
            break;
        };
        if now_ms.saturating_sub(m.created_at_ms) < retention_ms {
            break;
        }
        store
            .delete_all(m.dead.iter().map(|id| store.segment_path(*id)).collect())
            .await?;
        stats.segments += m.dead.len();
        // Delete the manifest last so a crash mid-way is retried next run.
        store.delete_all(vec![store.manifest_path(prev)]).await?;
        stats.manifests += 1;
        applied = m.frontier;
    }
    for (writer, upto) in applied {
        let paths: Vec<_> = store
            .list_wals(writer)
            .await?
            .into_iter()
            .filter(|seq| *seq <= upto)
            .map(|seq| store.wal_path(writer, seq))
            .collect();
        stats.wals += paths.len();
        store.delete_all(paths).await?;
    }
    Ok(stats)
}
