//! Deletes nodes, WALs and manifests that no reader can need anymore.
//!
//! Manifest `m[i]` lists the nodes it freed from `m[i-1]`. Once `m[i]` is older
//! than the retention, nobody may still be reading `m[i-1]` (readers must
//! refresh within the retention), so `m[i]`'s freed nodes, `m[i-1]` itself,
//! and WALs already folded into `m[i]` can go. The latest manifest is never
//! deleted.

use std::time::Duration;

use crate::error::Result;
use crate::manifest;
use crate::store::Store;
use crate::types::WalId;

#[derive(Clone, Copy, Debug)]
pub struct GcConfig {
    pub interval: Duration,
    /// Minimum age of a superseding manifest before the version it replaced is
    /// deleted. Must exceed the longest snapshot or reader refresh interval.
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
    pub nodes: usize,
    pub wals: usize,
}

pub async fn run(store: &Store, now_ms: u64, retention: Duration) -> Result<GcStats> {
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let ids = store.list_manifests().await?;
    let mut stats = GcStats::default();
    let mut wal_floor = WalId::NONE;
    for pair in ids.windows(2) {
        let (prev, cur) = (pair[0], pair[1]);
        let Some(m) = manifest::load(store, cur).await? else {
            break;
        };
        if now_ms.saturating_sub(m.created_at_ms) < retention_ms {
            break;
        }
        store
            .delete_all(m.freed.iter().map(|id| store.node_path(*id)).collect())
            .await?;
        stats.nodes += m.freed.len();
        // Delete the manifest last so a crash mid-way is retried next run.
        store.delete_all(vec![store.manifest_path(prev)]).await?;
        stats.manifests += 1;
        wal_floor = m.wal_replay_after;
    }
    let wals: Vec<_> = store
        .list_wals()
        .await?
        .into_iter()
        .filter(|id| *id <= wal_floor)
        .collect();
    stats.wals = wals.len();
    store
        .delete_all(wals.into_iter().map(|id| store.wal_path(id)).collect())
        .await?;
    Ok(stats)
}
