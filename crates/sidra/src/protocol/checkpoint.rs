//! One checkpoint by the applier, as typestates: each stage consumes the
//! result of the I/O the previous one asked for and says what to do next.
//!
//! ```text
//! Scanned (leases read, before pulling logs)
//!   --pulled--> Sealing (logs of expired writers to seal)
//!   --sealed--> Applying (messages, tombstone threshold, victims) | nothing to do
//!   --applied--> Publishing (the manifest to put-if-absent)
//! ```
//!
//! The order matters: leases are read before logs are pulled (an entry made
//! durable before its writer's lease is then visible to the pull), and seals
//! go in before the floor is computed (a sealed writer stops holding it).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::hlc::{Ts, WriterId};
use crate::manifest::{Frontier, Manifest};
use crate::protocol::applier::{self, hand_back};
use crate::protocol::follower::View;
use crate::protocol::tombstones::{self, Pull};
use crate::segments::{self, CleanerConfig, SegmentTable};
use crate::settings::ApplierRole;
use crate::types::{Epoch, SegmentId, TreeRoot, Version};
use crate::wal::Lease;

use bytes::Bytes;

/// Who is checkpointing and how.
#[derive(Clone, Copy, Debug)]
pub struct Applier<'a> {
    pub me: WriterId,
    pub epoch: Epoch,
    pub role: ApplierRole,
    pub cleaner: &'a CleanerConfig,
    /// `TreeConfig::purge_tombstones_after`.
    pub purge_after: Duration,
    /// `CheckpointConfig::max_bytes`.
    pub max_bytes: usize,
}

/// Leases read: every writer with a lease, and this writer's own floor.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scanned {
    pub own_floor: u64,
    pub leases: Vec<Lease>,
    /// Writers with a lease in storage.
    pub present: BTreeSet<WriterId>,
}

impl Scanned {
    /// Logs were pulled (`pull` says whether every one was reachable).
    pub fn pulled(self, view: &View, me: WriterId, now_ms: u64, pull: Pull) -> Sealing {
        let to_seal =
            tombstones::to_seal(&self.leases, me, now_ms, |w| view.is_sealed(w)).collect();
        Sealing {
            to_seal,
            scanned: self,
            pull,
        }
    }
}

/// Seal these logs (pulling their last entries), then call `sealed`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sealing {
    pub to_seal: Vec<WriterId>,
    scanned: Scanned,
    pull: Pull,
}

/// Whether a checkpoint left held entries for the next one
/// (`CheckpointConfig::max_bytes`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Left {
    Nothing,
    Entries,
}

/// What a checkpoint found to do.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Plan {
    /// No entries, seals, or segments to clean.
    Nothing,
    Apply(Applying),
}

impl Sealing {
    /// Seals are in (those that could be placed; an unreachable log stays
    /// unsealed and keeps holding the floor). `view` includes what sealing
    /// pulled.
    pub fn sealed(self, view: &View, a: Applier, now_ms: u64) -> Plan {
        let victims = segments::pick_victims(&view.manifest.segments, a.cleaner);
        let is_sealed = |w| view.is_sealed(w);
        let Scanned {
            own_floor,
            leases,
            present,
        } = self.scanned;
        let floor = tombstones::floor(own_floor, &leases, is_sealed);
        // With nothing new, purging still needs a checkpoint, or an idle
        // tree keeps its tombstones. Judged by the floor rather than the last
        // manifest's bound: when only that bound holds a purge back, this
        // checkpoint publishes a higher one and the next purges.
        let purge = self.pull == Pull::Complete
            && tombstones::purge_due(
                view.manifest.tree.and_then(|r| r.purge_at),
                floor,
                a.purge_after,
            );
        if view.tails.is_empty()
            && view.tails.sealed().next().is_none()
            && victims.is_empty()
            && !purge
        {
            return Plan::Nothing;
        }
        let taken = view.tails.take(a.max_bytes, &view.manifest.frontier);
        let drop_below = tombstones::drop_below(floor, view.manifest.tombstone_bound, self.pull);
        Plan::Apply(Applying {
            left: match taken.rest_oldest {
                Some(_) => Left::Entries,
                None => Left::Nothing,
            },
            msgs: taken.msgs,
            msg_bytes: taken.bytes,
            drop_below: taken.rest_oldest.map_or(drop_below, |t| drop_below.min(t)),
            victims,
            hand_back: hand_back(a.role, &leases, a.me, now_ms, is_sealed),
            floor,
            present,
            prev: view.manifest.clone(),
            frontier: taken.frontier,
            newly_sealed: taken.sealed,
        })
    }
}

/// Fold `msgs` into the tree, dropping tombstones below `drop_below` and
/// moving live data out of `victims`; then call `applied`.
///
/// Everything the published manifest records (the frontier, new seals) is
/// captured here with `msgs`, from one view: entries arriving later aren't
/// in this tree, so they mustn't count as applied.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Applying {
    pub msgs: Vec<(Bytes, Version)>,
    /// Log bytes of the batches `msgs` came from.
    pub msg_bytes: usize,
    pub left: Left,
    pub drop_below: Ts,
    pub victims: BTreeSet<SegmentId>,
    floor: u64,
    hand_back: bool,
    present: BTreeSet<WriterId>,
    prev: Arc<Manifest>,
    frontier: Frontier,
    newly_sealed: BTreeSet<WriterId>,
}

/// What the data plane produced for one checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Applied {
    pub tree: Option<TreeRoot>,
    pub segments: SegmentTable,
    pub dead: Vec<SegmentId>,
    pub segment_watermark: SegmentId,
}

impl Applying {
    /// The tree is rewritten and its segments uploaded.
    pub fn applied(self, a: Applier, done: Applied, now_ms: u64) -> Publishing {
        let prev: &Manifest = &self.prev;
        Publishing {
            manifest: applier::next_manifest(applier::Checkpoint {
                prev,
                epoch: a.epoch,
                me: a.me,
                tree: done.tree,
                frontier: self.frontier,
                newly_sealed: self.newly_sealed,
                present: &self.present,
                segment_watermark: done.segment_watermark,
                tombstone_bound: tombstones::next_bound(prev.tombstone_bound, self.floor),
                segments: done.segments,
                dead: done.dead,
                now_ms,
            }),
            hand_back: self.hand_back,
        }
    }
}

/// Put-if-absent `manifest` as the next version. If published and
/// `hand_back`, release the role (a standby giving it back to a primary).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Publishing {
    pub manifest: Manifest,
    pub hand_back: bool,
}
