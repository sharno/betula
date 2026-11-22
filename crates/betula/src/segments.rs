//! Segment space accounting and cleaning policy.
//!
//! The manifest tracks, for every segment the tree references, its total size
//! and how many of its bytes are still reachable. A checkpoint adds its new
//! segment and subtracts every node it freed. A segment at zero live bytes is
//! dead and is deleted by GC after retention. Segments that are mostly dead are
//! cleaned: the next checkpoint rewrites their live nodes, which drives them to
//! zero.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::types::{NodePtr, SegmentId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentUsage {
    pub total: u64,
    pub live: u64,
}

pub type SegmentTable = BTreeMap<SegmentId, SegmentUsage>;

#[derive(Clone, Copy, Debug)]
pub struct CleanerConfig {
    /// Clean segments whose live fraction is below this.
    pub clean_below_live: f64,
    /// Cap on live bytes moved per checkpoint, which bounds the extra work any
    /// one checkpoint does. At least one victim is always taken.
    pub max_moved_bytes: u64,
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            // Cleaning at live fraction t adds about t/(1-t) to write
            // amplification and bounds space amplification near 1/t.
            clean_below_live: 0.25,
            max_moved_bytes: 64 << 20,
        }
    }
}

/// The table after a checkpoint that wrote `new` (id, size) segments and
/// freed `freed`, plus the segments that just died.
pub fn account(
    prev: &SegmentTable,
    new: &[(SegmentId, u64)],
    freed: &[NodePtr],
) -> (SegmentTable, Vec<SegmentId>) {
    let mut table = prev.clone();
    table.extend(
        new.iter()
            .map(|&(id, total)| (id, SegmentUsage { total, live: total })),
    );
    freed.iter().for_each(|ptr| {
        let usage = table
            .get_mut(&ptr.seg)
            .expect("a freed node lives in a tracked segment");
        debug_assert!(
            usage.live >= u64::from(ptr.len),
            "segment {} over-freed",
            ptr.seg
        );
        usage.live = usage.live.saturating_sub(u64::from(ptr.len));
    });
    let dead: Vec<SegmentId> = table
        .iter()
        .filter(|(_, u)| u.live == 0)
        .map(|(id, _)| *id)
        .collect();
    dead.iter().for_each(|id| {
        table.remove(id);
    });
    (table, dead)
}

/// The emptiest segments below the live threshold, up to the move budget.
pub fn pick_victims(table: &SegmentTable, cfg: &CleanerConfig) -> HashSet<SegmentId> {
    let mut candidates: Vec<(SegmentId, SegmentUsage)> = table
        .iter()
        .filter(|(_, u)| (u.live as f64) < cfg.clean_below_live * u.total as f64)
        .map(|(id, u)| (*id, *u))
        .collect();
    candidates.sort_by(|(_, a), (_, b)| {
        let ratio = |u: &SegmentUsage| u.live as f64 / u.total.max(1) as f64;
        ratio(a).total_cmp(&ratio(b))
    });
    candidates
        .into_iter()
        .scan(0u64, |moved, (id, u)| {
            let first = *moved == 0;
            *moved += u.live;
            (first || *moved <= cfg.max_moved_bytes).then_some(id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Epoch;

    fn seg(seq: u64) -> SegmentId {
        SegmentId {
            epoch: Epoch(1),
            seq,
        }
    }

    fn ptr(seq: u64, offset: u64, len: u32) -> NodePtr {
        NodePtr {
            seg: seg(seq),
            offset,
            len,
        }
    }

    #[test]
    fn freeing_everything_kills_a_segment() {
        let (t, dead) = account(&SegmentTable::new(), &[(seg(0), 30)], &[]);
        assert!(dead.is_empty());
        let (t, dead) = account(&t, &[(seg(1), 10)], &[ptr(0, 0, 10), ptr(0, 10, 20)]);
        assert_eq!(dead, vec![seg(0)]);
        assert_eq!(t.keys().copied().collect::<Vec<_>>(), vec![seg(1)]);
    }

    #[test]
    fn victims_are_emptiest_first_within_budget() {
        let table: SegmentTable = [
            (
                seg(0),
                SegmentUsage {
                    total: 100,
                    live: 40,
                },
            ),
            (
                seg(1),
                SegmentUsage {
                    total: 100,
                    live: 10,
                },
            ),
            (
                seg(2),
                SegmentUsage {
                    total: 100,
                    live: 90,
                },
            ),
            (
                seg(3),
                SegmentUsage {
                    total: 100,
                    live: 30,
                },
            ),
        ]
        .into();
        let cfg = CleanerConfig {
            clean_below_live: 0.5,
            max_moved_bytes: 45,
        };
        assert_eq!(pick_victims(&table, &cfg), [seg(1), seg(3)].into());
        let tight = CleanerConfig {
            max_moved_bytes: 1,
            ..cfg
        };
        assert_eq!(pick_victims(&table, &tight), [seg(1)].into());
    }
}
