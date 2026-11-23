//! The applier role: when to take it, when to checkpoint, when a standby
//! hands it back, and what a checkpoint publishes.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::hlc::WriterId;
use crate::manifest::{Frontier, Manifest};
use crate::segments::SegmentTable;
use crate::settings::{ApplierRole, Candidacy, CheckpointConfig};
use crate::types::{Epoch, ManifestId, SegmentId, TreeRoot};
use crate::wal::Lease;

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// What a writer not holding the role sees.
#[derive(Clone, Copy, Debug)]
pub struct Situation<'a> {
    pub me: WriterId,
    pub now_ms: u64,
    pub manifest_id: ManifestId,
    pub manifest: &'a Manifest,
    /// When the oldest entry not in the tree arrived, if any.
    pub waiting_since_ms: Option<u64>,
    pub checkpoint_interval: Duration,
}

impl Situation<'_> {
    fn waited(&self, d: Duration) -> bool {
        self.waiting_since_ms
            .is_some_and(|t| self.now_ms >= t.saturating_add(ms(d)))
    }

    /// No manifest was published for `d`.
    fn idle_for(&self, d: Duration) -> bool {
        self.now_ms >= self.manifest.created_at_ms.saturating_add(ms(d))
    }
}

/// Whether to take the applier role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    No,
    Yes,
    /// Take it if this writer's lease ran out (see [`lease_expired`]).
    IfLeaseExpired(WriterId),
}

pub fn claim(role: ApplierRole, s: &Situation) -> Claim {
    let steal_after = match role {
        ApplierRole::Never => return Claim::No,
        // Whatever the reason nobody applied (no applier, stuck, dead), the
        // home region had `after` to do it.
        ApplierRole::Standby { after } => {
            return match s.waited(after) && s.idle_for(after) && s.manifest.applier != Some(s.me) {
                true => Claim::Yes,
                false => Claim::No,
            };
        }
        ApplierRole::Auto { steal_after } => steal_after,
    };
    match s.manifest.applier {
        _ if s.manifest_id == ManifestId(0) => Claim::Yes,
        // Released on close: take it as soon as there's work.
        None if s.waiting_since_ms.is_some() => Claim::Yes,
        None => Claim::No,
        Some(w) if w == s.me => Claim::No,
        // Alive but stuck: work waited past `steal_after` and the applier
        // published nothing meanwhile.
        Some(_) if s.waited(steal_after) && s.idle_for(steal_after) => Claim::Yes,
        // Work waited a couple of checkpoint intervals: take over at once if
        // the applier's lease ran out (it crashed or lost the store).
        Some(w) if s.waited(s.checkpoint_interval * 2) => Claim::IfLeaseExpired(w),
        Some(_) => Claim::No,
    }
}

/// A missing lease counts as expired.
pub fn lease_expired(lease: Option<&Lease>, now_ms: u64) -> bool {
    lease.is_none_or(|l| l.expires_at_ms <= now_ms)
}

/// Whether the applier should checkpoint now.
pub fn due(
    cfg: &CheckpointConfig,
    unapplied_bytes: usize,
    waiting_since_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    unapplied_bytes >= cfg.bytes
        || waiting_since_ms.is_some_and(|t| now_ms >= t.saturating_add(ms(cfg.interval)))
}

/// A standby applier hands the role back while a primary is alive.
pub fn hand_back(
    role: ApplierRole,
    leases: &[Lease],
    me: WriterId,
    now_ms: u64,
    is_sealed: impl Fn(WriterId) -> bool,
) -> bool {
    matches!(role, ApplierRole::Standby { .. })
        && leases.iter().any(|l| {
            l.candidacy == Candidacy::Primary
                && l.writer != me
                && !is_sealed(l.writer)
                && l.expires_at_ms > now_ms
        })
}

/// What one checkpoint produced, for the manifest it publishes.
pub struct Checkpoint<'a> {
    pub prev: &'a Manifest,
    pub epoch: Epoch,
    pub me: WriterId,
    pub tree: Option<TreeRoot>,
    /// The frontier including every entry in this checkpoint.
    pub frontier: Frontier,
    /// Writers whose logs this checkpoint read up to their seal.
    pub newly_sealed: BTreeSet<WriterId>,
    /// Writers with a lease in storage.
    pub present: &'a BTreeSet<WriterId>,
    pub segment_watermark: SegmentId,
    pub tombstone_bound: u64,
    pub segments: SegmentTable,
    pub dead: Vec<SegmentId>,
    pub now_ms: u64,
}

/// The manifest a checkpoint publishes. Writers GC forgot (sealed, lease
/// deleted) can never write again, so they leave the frontier and the
/// sealed set.
pub fn next_manifest(c: Checkpoint) -> Manifest {
    let present = c.present;
    Manifest {
        applier_epoch: c.epoch,
        applier: Some(c.me),
        tree: c.tree,
        frontier: c
            .frontier
            .into_iter()
            .filter(|(w, _)| present.contains(w))
            .collect(),
        sealed: c
            .prev
            .sealed
            .iter()
            .copied()
            .chain(c.newly_sealed)
            .filter(|w| present.contains(w))
            .collect(),
        segment_watermark: c.segment_watermark,
        tombstone_bound: c.tombstone_bound,
        segments: c.segments,
        dead: c.dead,
        created_at_ms: c.now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::LogHome;

    const SEC: Duration = Duration::from_secs(1);

    fn situation(m: &Manifest, me: u64, now_ms: u64, waiting: Option<u64>) -> Situation<'_> {
        Situation {
            me: WriterId(me),
            now_ms,
            manifest_id: ManifestId(5),
            manifest: m,
            waiting_since_ms: waiting,
            checkpoint_interval: 2 * SEC,
        }
    }

    fn held_by(w: u64, created_at_ms: u64) -> Manifest {
        Manifest {
            applier: Some(WriterId(w)),
            ..Manifest::initial(created_at_ms)
        }
    }

    #[test]
    fn auto_claims_free_stuck_and_maybe_dead_roles() {
        let auto = ApplierRole::Auto {
            steal_after: 10 * SEC,
        };
        let free = Manifest::initial(0);
        assert_eq!(claim(auto, &situation(&free, 1, 100, Some(50))), Claim::Yes);
        assert_eq!(claim(auto, &situation(&free, 1, 100, None)), Claim::No);
        let other = held_by(2, 0);
        assert_eq!(
            claim(auto, &situation(&other, 1, 1_000, Some(900))),
            Claim::No
        );
        assert_eq!(
            claim(auto, &situation(&other, 1, 5_000, Some(0))),
            Claim::IfLeaseExpired(WriterId(2))
        );
        assert_eq!(
            claim(auto, &situation(&other, 1, 11_000, Some(0))),
            Claim::Yes
        );
        let mine = held_by(1, 0);
        assert_eq!(
            claim(auto, &situation(&mine, 1, 99_000, Some(0))),
            Claim::No
        );
        let empty = Situation {
            manifest_id: ManifestId(0),
            ..situation(&other, 1, 0, None)
        };
        assert_eq!(claim(auto, &empty), Claim::Yes);
    }

    #[test]
    fn standby_waits_and_never_takes_from_itself() {
        let standby = ApplierRole::Standby { after: 30 * SEC };
        let other = held_by(2, 0);
        assert_eq!(
            claim(standby, &situation(&other, 1, 20_000, Some(0))),
            Claim::No
        );
        assert_eq!(
            claim(standby, &situation(&other, 1, 31_000, Some(0))),
            Claim::Yes
        );
        let mine = held_by(1, 0);
        assert_eq!(
            claim(standby, &situation(&mine, 1, 31_000, Some(0))),
            Claim::No
        );
        assert_eq!(
            claim(ApplierRole::Never, &situation(&other, 1, 99_000, Some(0))),
            Claim::No
        );
    }

    #[test]
    fn standby_hands_back_to_a_live_unsealed_primary() {
        let standby = ApplierRole::Standby { after: SEC };
        let lease = |w, candidacy, exp| Lease {
            writer: WriterId(w),
            log: LogHome::Home,
            candidacy,
            floor_hlc: 0,
            expires_at_ms: exp,
        };
        let leases = [
            lease(2, Candidacy::Primary, 500),
            lease(3, Candidacy::Standby, 900),
        ];
        assert!(hand_back(standby, &leases, WriterId(1), 100, |_| false));
        assert!(!hand_back(standby, &leases, WriterId(1), 600, |_| false));
        assert!(!hand_back(standby, &leases, WriterId(1), 100, |w| w == WriterId(2)));
        let auto = ApplierRole::Auto { steal_after: SEC };
        assert!(!hand_back(auto, &leases, WriterId(1), 100, |_| false));
    }

    #[test]
    fn forgotten_writers_leave_the_manifest() {
        let prev = Manifest {
            sealed: [WriterId(7), WriterId(8)].into(),
            ..Manifest::initial(0)
        };
        let present: BTreeSet<_> = [WriterId(1), WriterId(8), WriterId(9)].into();
        let m = next_manifest(Checkpoint {
            prev: &prev,
            epoch: Epoch(3),
            me: WriterId(1),
            tree: None,
            frontier: [(WriterId(1), 4), (WriterId(7), 2), (WriterId(9), 1)].into(),
            newly_sealed: [WriterId(9)].into(),
            present: &present,
            segment_watermark: SegmentId {
                epoch: Epoch(3),
                seq: 2,
            },
            tombstone_bound: 40,
            segments: SegmentTable::new(),
            dead: Vec::new(),
            now_ms: 10,
        });
        assert_eq!(m.frontier, [(WriterId(1), 4), (WriterId(9), 1)].into());
        assert_eq!(m.sealed, [WriterId(8), WriterId(9)].into());
        assert_eq!(m.applier, Some(WriterId(1)));
    }
}
