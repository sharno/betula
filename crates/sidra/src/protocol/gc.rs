//! What garbage collection may delete.
//!
//! Readers must refresh within the retention. So for each manifest m[i]
//! older than the retention, nobody can still be reading m[i-1]: its dead
//! segments, the manifest itself, and log slots m[i] applied can go, except
//! fences.
//!
//! A writer stalled past the retention may still put a manifest into an id
//! GC collected. It finds a newer manifest listed right after and stops,
//! but its stray stays until GC deletes it. The manifest above names
//! another opening as its parent, so GC deletes the stray without acting on
//! its dead segments.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::manifest::Manifest;
use crate::types::{ManifestId, WalSeq};

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether manifest `m` is past the retention, so its predecessor can go.
pub fn supersedes_collectably(m: &Manifest, now_ms: u64, retention: Duration) -> bool {
    now_ms.saturating_sub(m.created_at_ms) >= ms(retention)
}

/// What GC does with two manifests listed one after the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pair {
    /// `prev` isn't `cur`'s parent: a stalled writer put it into an id GC
    /// had collected. Nobody reads it; it goes, and nothing else with it.
    Stray,
    /// `cur` is past the retention: its dead segments and `prev` go.
    Collect,
    /// `cur` is within the retention: GC stops here.
    Keep,
}

/// Classifies manifests `prev` and `cur`, listed one after the other.
/// Published manifests have consecutive ids, each naming the one below as
/// its parent; strays sit below them all, in ids GC collected.
pub fn pair(
    prev: (ManifestId, &Manifest),
    cur: (ManifestId, &Manifest),
    now_ms: u64,
    retention: Duration,
) -> Pair {
    let parent = cur.0 == prev.0.next() && cur.1.parent == prev.1.writer;
    match (parent, supersedes_collectably(cur.1, now_ms, retention)) {
        (false, _) => Pair::Stray,
        (true, true) => Pair::Collect,
        (true, false) => Pair::Keep,
    }
}

/// The log slots in `stored` that manifest `m` (past the retention) lets go:
/// applied, and not a fence.
pub fn deletable_wals<'a>(
    stored: &'a [WalSeq],
    m: &'a Manifest,
) -> impl Iterator<Item = WalSeq> + 'a {
    let keep: &BTreeSet<WalSeq> = &m.fences;
    stored
        .iter()
        .copied()
        .filter(move |s| *s <= m.wal_applied && !keep.contains(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WriterId;

    #[test]
    fn manifests_age_out_after_the_retention() {
        let m = Manifest::initial(1_000);
        let ret = Duration::from_secs(1);
        assert!(!supersedes_collectably(&m, 1_999, ret));
        assert!(supersedes_collectably(&m, 2_000, ret));
    }

    #[test]
    fn a_manifest_its_successor_does_not_name_is_a_stray() {
        let ret = Duration::from_secs(1);
        let by = |writer: u64, parent: u64| Manifest {
            writer: WriterId(writer),
            parent: WriterId(parent),
            ..Manifest::initial(0)
        };
        let id = ManifestId;
        // Opening A checkpointed 4 on top of 3; opening B claimed 5.
        let (three, four, five) = (by(1, 1), by(1, 1), by(2, 1));
        assert_eq!(
            pair((id(3), &three), (id(4), &four), 5_000, ret),
            Pair::Collect
        );
        assert_eq!(pair((id(4), &four), (id(5), &five), 500, ret), Pair::Keep);
        // A, stalled, put 4 again after GC deleted it; B's 5 names B's 4.
        let (stray, five) = (by(1, 1), by(2, 2));
        assert_eq!(
            pair((id(4), &stray), (id(5), &five), 5_000, ret),
            Pair::Stray
        );
        // A gap below a manifest means what's under it is a stray.
        assert_eq!(
            pair((id(2), &four), (id(4), &four), 5_000, ret),
            Pair::Stray
        );
        // A and C both read 0 and claimed 1 with the same epoch; A won.
        // Past GC, C's claim and A's checkpoint land in 1 and 2: same
        // epochs as a real chain, but 2 names A, not C.
        let (c_claim, a_checkpoint) = (by(3, 0), by(1, 1));
        assert_eq!(
            pair((id(1), &c_claim), (id(2), &a_checkpoint), 5_000, ret),
            Pair::Stray
        );
    }

    #[test]
    fn applied_slots_go_but_fences_stay() {
        let m = Manifest {
            wal_applied: WalSeq(5),
            fences: [WalSeq(3)].into(),
            ..Manifest::initial(0)
        };
        let stored: Vec<WalSeq> = (1..=7).map(WalSeq).collect();
        let gone: Vec<u64> = deletable_wals(&stored, &m).map(|s| s.0).collect();
        assert_eq!(gone, [1, 2, 4, 5]);
    }
}
