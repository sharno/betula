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
use crate::store::{Pace, Store};

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

pub async fn run(store: &Store, now_ms: u64, retention: Duration, speed: Speed) -> Result<GcStats> {
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
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
        if now_ms.saturating_sub(m.created_at_ms) < retention_ms {
            break;
        }
        segments.extend(m.dead.iter().map(|id| store.segment_path(*id)));
        manifests.push(store.manifest_path(prev));
        applied = m.frontier;
    }
    let wals: Vec<_> =
        futures::future::try_join_all(applied.into_iter().map(|(writer, upto)| async move {
            Ok::<_, crate::error::Error>(
                store
                    .list_wals(writer)
                    .await?
                    .into_iter()
                    .filter(move |seq| *seq <= upto)
                    .map(move |seq| store.wal_path(writer, seq)),
            )
        }))
        .await?
        .into_iter()
        .flatten()
        .collect();
    stats.segments = segments.len();
    stats.wals = wals.len();
    stats.manifests = manifests.len();
    // Manifests go last, so a crash mid-way leaves them to retry the run.
    let all: Vec<_> = segments.into_iter().chain(wals).chain(manifests).collect();
    let pace = match speed {
        Speed::Full => Pace::Unlimited,
        Speed::Spread { over, max_per_sec } => {
            let needed = (all.len() as f64 / over.as_secs_f64().max(1.0)).ceil() as u32;
            Pace::PerSecond(needed.clamp(1, max_per_sec.max(1)))
        }
    };
    store.delete_paced(all, pace).await?;
    stats.writers = forget_writers(store, now_ms, retention_ms).await?;
    Ok(stats)
}

/// Deletes the leases of writers that expired more than `retention_ms` ago
/// and have no log objects left. Such a writer stopped writing long ago (an
/// expired writer never writes again), so nothing can need its lease; once
/// it's gone the applier also drops the writer from the manifest.
async fn forget_writers(store: &Store, now_ms: u64, retention_ms: u64) -> Result<usize> {
    let writers = store.list_writers().await?;
    let gone: Vec<_> = futures::future::try_join_all(writers.into_iter().map(|w| async move {
        let lease = wal::read_lease(store, w).await?;
        let expired_long_ago =
            lease.is_some_and(|l| now_ms.saturating_sub(l.expires_at_ms) > retention_ms);
        let forget = expired_long_ago && store.list_wals(w).await?.is_empty();
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
