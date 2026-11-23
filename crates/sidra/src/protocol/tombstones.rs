//! When tombstones may be dropped, and which logs to seal first.
//!
//! A tombstone can go once no writer can still produce an older version of
//! its key. Older versions already in the tree are carried down to the leaf
//! whenever it's rewritten, and so are the checkpoint's own entries. So the
//! question is only about versions not yet in the tree:
//!
//! - A writer with a lease stays above its lease's floor until its log is
//!   sealed; then all its entries are in the checkpoint or applied, and it
//!   can write no more. Expiry alone proves nothing (a write may be in
//!   flight), so a stale lease holds the floor back until sealed.
//! - A writer whose lease the scan missed wrote it after the previous
//!   manifest was published, then read a manifest at least that new and
//!   moved its clock past that manifest's `tombstone_bound` before writing.
//!   So the bound published last time caps what's safe to drop now.
//! - A writer whose log couldn't be pulled may hold older entries: drop
//!   nothing until it's reachable.

use crate::hlc::{Ts, WriterId};
use crate::wal::Lease;

/// Leases of other writers that ran out and whose logs aren't sealed yet.
pub fn to_seal<'a>(
    leases: &'a [Lease],
    me: WriterId,
    now_ms: u64,
    is_sealed: impl Fn(WriterId) -> bool + 'a,
) -> impl Iterator<Item = WriterId> + 'a {
    leases
        .iter()
        .filter(move |l| l.expires_at_ms <= now_ms && l.writer != me && !is_sealed(l.writer))
        .map(|l| l.writer)
}

/// The lowest floor among unsealed leased writers and this writer.
pub fn floor(own_floor: u64, leases: &[Lease], is_sealed: impl Fn(WriterId) -> bool) -> u64 {
    leases
        .iter()
        .filter(|l| !is_sealed(l.writer))
        .map(|l| l.floor_hlc)
        .fold(own_floor, u64::min)
}

/// Whether every log the checkpoint needed was pulled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pull {
    Complete,
    /// Some writer's log store couldn't be reached.
    Partial,
}

/// Tombstones older than this may be dropped by this checkpoint.
pub fn drop_below(floor: u64, prev_bound: u64, pull: Pull) -> Ts {
    match pull {
        Pull::Complete => Ts {
            hlc: floor.min(prev_bound),
            writer: WriterId(0),
        },
        Pull::Partial => Ts::MIN,
    }
}

/// The bound the next manifest publishes. Never decreases.
pub fn next_bound(prev_bound: u64, floor: u64) -> u64 {
    prev_bound.max(floor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::LogHome;
    use crate::settings::Candidacy;

    fn lease(writer: u64, floor_hlc: u64, expires_at_ms: u64) -> Lease {
        Lease {
            writer: WriterId(writer),
            log: LogHome::Home,
            candidacy: Candidacy::Primary,
            floor_hlc,
            expires_at_ms,
        }
    }

    #[test]
    fn stale_leases_hold_the_floor_until_sealed() {
        let leases = [lease(1, 50, 10), lease(2, 80, 1_000)];
        assert_eq!(floor(100, &leases, |_| false), 50);
        assert_eq!(floor(100, &leases, |w| w == WriterId(1)), 80);
        assert_eq!(floor(60, &leases, |_| true), 60);
    }

    #[test]
    fn seals_only_other_expired_unsealed_writers() {
        let leases = [lease(1, 0, 10), lease(2, 0, 1_000), lease(3, 0, 10)];
        let me = WriterId(3);
        let got: Vec<_> = to_seal(&leases, me, 500, |_| false).collect();
        assert_eq!(got, [WriterId(1)]);
        assert_eq!(to_seal(&leases, me, 500, |_| true).count(), 0);
    }

    #[test]
    fn drops_below_the_previous_bound_and_nothing_on_partial_pulls() {
        assert_eq!(drop_below(90, 70, Pull::Complete).hlc, 70);
        assert_eq!(drop_below(60, 70, Pull::Complete).hlc, 60);
        assert_eq!(drop_below(90, 70, Pull::Partial), Ts::MIN);
        assert_eq!(next_bound(70, 60), 70);
        assert_eq!(next_bound(70, 90), 90);
    }
}
