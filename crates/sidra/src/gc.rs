//! Deletes segments, writer log entries and manifests that no reader can
//! need anymore.
//!
//! Manifest `m[i]` lists the segments that died going from `m[i-1]` to `m[i]`.
//! Once `m[i]` is older than the retention, nobody may still be reading
//! `m[i-1]` (readers must refresh within the retention), so `m[i]`'s dead
//! segments, `m[i-1]` itself, and writer log entries already folded into
//! `m[i]` can go. The latest manifest is never deleted.
//!
//! Log seals are never deleted: a writer id's seal is what keeps it from
//! writing again (see `wal`). They live apart from leases, so the writers
//! everyone lists and polls are only the ones that still matter.

use std::time::Duration;

use crate::error::Result;
use crate::logs::Logs;
use crate::manifest::{self, Frontier, Manifest};
use crate::protocol;
use crate::store::{Pace, Store};
use object_store::path::Path;

use crate::wal;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct GcConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Minimum age of a superseding manifest before the version it replaced is
    /// deleted. Must exceed the longest snapshot or reader refresh interval.
    #[serde(with = "humantime_serde")]
    pub retention: Duration,
    /// Ceiling on deletes per second. Each run spreads its deletes evenly
    /// over the interval, so the rate adapts to the garbage produced; bursts
    /// of deletes otherwise compete with writers' log PUTs and stall them.
    pub max_deletes_per_sec: u32,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            retention: Duration::from_secs(600),
            max_deletes_per_sec: 100,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    pub manifests: usize,
    pub segments: usize,
    pub wals: usize,
    pub writers: usize,
}

/// How fast a GC run deletes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Speed {
    /// As fast as the store allows (explicit runs).
    Full,
    /// Spread evenly over `over`, so deletes trickle instead of bursting,
    /// but never faster than `max_per_sec`.
    Spread { over: Duration, max_per_sec: u32 },
}

pub async fn run(logs: &Logs, now_ms: u64, retention: Duration, speed: Speed) -> Result<GcStats> {
    let store = logs.home();
    let ids = store.list_manifests().await?;
    let mut stats = GcStats::default();
    let mut applied = Frontier::new();
    let mut segments = Vec::new();
    let mut manifests = Vec::new();
    for pair in ids.windows(2) {
        let (prev, cur) = (pair[0], pair[1]);
        let Some(m) = manifest::load(store, cur).await? else {
            break;
        };
        if !protocol::gc::supersedes_collectably(&m, now_ms, retention) {
            break;
        }
        segments.extend(m.dead.iter().map(|id| store.segment_path(*id)));
        manifests.push(store.manifest_path(prev));
        applied = m.frontier;
    }
    // Log objects may live in regional stores, so deletes go per store.
    let wals: Vec<(&Store, Vec<Path>)> =
        futures::future::try_join_all(applied.into_iter().map(|(writer, upto)| async move {
            let at = logs.of(writer).await?;
            let paths = at
                .list_wals(writer)
                .await?
                .into_iter()
                .filter(|seq| *seq <= upto)
                .map(|seq| at.wal_path(writer, seq))
                .collect();
            Ok::<_, crate::error::Error>((at, paths))
        }))
        .await?;
    stats.segments = segments.len();
    stats.wals = wals.iter().map(|(_, p)| p.len()).sum();
    stats.manifests = manifests.len();
    // Manifests go last, so a crash mid-way leaves them to retry the run.
    let groups: Vec<(&Store, Vec<Path>)> = [(store, segments)]
        .into_iter()
        .chain(wals)
        .chain([(store, manifests)])
        .collect();
    let total: usize = groups.iter().map(|(_, p)| p.len()).sum();
    let pace = match speed {
        Speed::Full => Pace::Unlimited,
        Speed::Spread { over, max_per_sec } => {
            let needed = (total as f64 / over.as_secs_f64().max(1.0)).ceil() as u32;
            Pace::PerSecond(needed.clamp(1, max_per_sec.max(1)))
        }
    };
    for (at, paths) in groups {
        at.delete_paced(paths, pace).await?;
    }
    let latest = match ids.last() {
        Some(id) => manifest::load(store, *id).await?,
        None => None,
    };
    stats.writers = match latest {
        Some(m) => forget_writers(logs, &m, now_ms, retention).await?,
        None => 0,
    };
    Ok(stats)
}

/// Deletes the leases of writers whose log `latest` records as sealed (so
/// fully applied), once the lease expired more than `retention` ago
/// (readers of older manifests may still look up where the log lives) and
/// only the seal is left of the log (nothing deletes applied entries once
/// the writer leaves the manifest). The applier then drops the writer from
/// the manifest; its seal stays.
async fn forget_writers(
    logs: &Logs,
    latest: &Manifest,
    now_ms: u64,
    retention: Duration,
) -> Result<usize> {
    let store = logs.home();
    let gone: Vec<_> = futures::future::try_join_all(latest.sealed.iter().map(|&w| async move {
        let applied = latest.frontier.get(&w).copied().unwrap_or(0);
        let lease = wal::read_lease(store, w).await?;
        // Listing the log only matters once the lease is old enough.
        let remaining = match &lease {
            Some(l) if protocol::gc::forgettable(Some(l), applied, &[], now_ms, retention) => {
                logs.at(&l.log)?.list_wals(w).await?
            }
            Some(_) | None => return Ok(None),
        };
        let forget =
            protocol::gc::forgettable(lease.as_ref(), applied, &remaining, now_ms, retention);
        Ok::<_, crate::error::Error>(forget.then(|| store.lease_path(w)))
    }))
    .await?
    .into_iter()
    .flatten()
    .collect();
    let n = gone.len();
    store.delete_all(gone).await?;
    Ok(n)
}
