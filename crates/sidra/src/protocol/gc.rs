//! What garbage collection may delete.
//!
//! Readers must refresh within the retention. So for each manifest m[i]
//! older than the retention, nobody can still be reading m[i-1]: its dead
//! segments, the manifest itself, and log entries m[i] applied can go.

use std::time::Duration;

use crate::manifest::Manifest;
use crate::wal::Lease;

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether manifest `m` is past the retention, so its predecessor can go.
pub fn supersedes_collectably(m: &Manifest, now_ms: u64, retention: Duration) -> bool {
    now_ms.saturating_sub(m.created_at_ms) >= ms(retention)
}

/// Whether a writer the latest manifest records as sealed can be forgotten
/// (its lease deleted): the lease expired more than the retention ago (a
/// reader of an older manifest may still look up where the log lives), and
/// only the seal is left of its log (`remaining` are the sequence numbers
/// still in storage; everything at or below `applied` was applied). Its seal
/// stays forever, so the writer id can never write again.
pub fn forgettable(
    lease: Option<&Lease>,
    applied: u64,
    remaining: &[u64],
    now_ms: u64,
    retention: Duration,
) -> bool {
    lease.is_some_and(|l| now_ms.saturating_sub(l.expires_at_ms) > ms(retention))
        && remaining.iter().all(|seq| *seq > applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::WriterId;
    use crate::logs::LogHome;
    use crate::settings::Candidacy;

    #[test]
    fn forgets_only_long_expired_writers_with_nothing_but_the_seal() {
        let lease = Lease {
            writer: WriterId(1),
            log: LogHome::Home,
            candidacy: Candidacy::Primary,
            floor_hlc: 0,
            expires_at_ms: 1_000,
        };
        let ret = Duration::from_secs(60);
        assert!(forgettable(Some(&lease), 5, &[6], 62_000, ret));
        assert!(!forgettable(Some(&lease), 5, &[5, 6], 62_000, ret));
        assert!(!forgettable(Some(&lease), 5, &[6], 30_000, ret));
        assert!(!forgettable(None, 5, &[6], 62_000, ret));
    }

    #[test]
    fn manifests_age_out_after_the_retention() {
        let m = Manifest::initial(1_000);
        let ret = Duration::from_secs(1);
        assert!(!supersedes_collectably(&m, 1_999, ret));
        assert!(supersedes_collectably(&m, 2_000, ret));
    }
}
