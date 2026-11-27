//! Deletes segments, log slots and manifests that no reader can need
//! anymore.
//!
//! Manifest `m[i]` lists the segments that died going from `m[i-1]` to `m[i]`.
//! Once `m[i]` is older than the retention, nobody may still be reading
//! `m[i-1]` (readers must refresh within the retention), so `m[i]`'s dead
//! segments, `m[i-1]` itself, and log slots already folded into `m[i]` can
//! go. The latest manifest and log fences are never deleted. Manifests a
//! stalled writer put into collected ids go at once (see `protocol::gc`).

use std::time::Duration;

use crate::error::Result;
use crate::manifest::{self, Manifest};
use crate::protocol::{self, gc::Pair};
use crate::store::{Pace, Store};
use crate::types::ManifestId;

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
    /// of deletes otherwise compete with log PUTs and stall them.
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
    let ids = store.list_manifests().await?;
    let mut segments = Vec::new();
    let mut manifests = Vec::new();
    let mut applied: Option<Manifest> = None;
    let mut prev: Option<(ManifestId, Manifest)> = None;
    for id in ids {
        let Some(cur) = manifest::load(store, id).await? else {
            break;
        };
        if let Some((prev_id, prev)) = &prev {
            match protocol::gc::pair((*prev_id, prev), (id, &cur), now_ms, retention) {
                Pair::Stray => manifests.push(*prev_id),
                Pair::Collect => {
                    segments.extend(cur.dead.iter().map(|id| store.segment_path(*id)));
                    manifests.push(*prev_id);
                    applied = Some(cur.clone());
                }
                Pair::Keep => break,
            }
        }
        prev = Some((id, cur));
    }
    let wals: Vec<_> = match &applied {
        Some(m) => protocol::gc::deletable_wals(&store.list_wals().await?, m)
            .map(|seq| store.wal_path(seq))
            .collect(),
        None => Vec::new(),
    };
    let stats = GcStats {
        manifests: manifests.len(),
        segments: segments.len(),
        wals: wals.len(),
    };
    let pace = match speed {
        Speed::Full => Pace::Unlimited,
        Speed::Spread { over, max_per_sec } => {
            let total = segments.len() + wals.len();
            let needed = (total as f64 / over.as_secs_f64().max(1.0)).ceil() as u32;
            Pace::PerSecond(needed.clamp(1, max_per_sec.max(1)))
        }
    };
    store
        .delete_paced(segments.into_iter().chain(wals).collect(), pace)
        .await?;
    // Manifests go last, so a crash mid-way leaves them to retry the run,
    // and in order, one at a time: an id is gone only once every id below
    // it is. A stalled writer's put can then land in a collected id only
    // below every published manifest, where GC sees it as a stray.
    for id in manifests {
        store.delete_all(vec![store.manifest_path(id)]).await?;
    }
    Ok(stats)
}
