//! What garbage collection may delete.
//!
//! Readers must refresh within the retention. So for each manifest m[i]
//! older than the retention, nobody can still be reading m[i-1]: its dead
//! segments, the manifest itself, and log slots m[i] applied can go, except
//! fences.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::manifest::Manifest;
use crate::types::WalSeq;

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether manifest `m` is past the retention, so its predecessor can go.
pub fn supersedes_collectably(m: &Manifest, now_ms: u64, retention: Duration) -> bool {
    now_ms.saturating_sub(m.created_at_ms) >= ms(retention)
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

    #[test]
    fn manifests_age_out_after_the_retention() {
        let m = Manifest::initial(1_000);
        let ret = Duration::from_secs(1);
        assert!(!supersedes_collectably(&m, 1_999, ret));
        assert!(supersedes_collectably(&m, 2_000, ret));
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
