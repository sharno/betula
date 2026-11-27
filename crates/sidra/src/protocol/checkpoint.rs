//! When the writer checkpoints, what one applies, and what it publishes.

use std::collections::BTreeSet;
use std::time::Duration;

use crate::hlc::Ts;
use crate::manifest::Manifest;
use crate::protocol::tail::{Tail, Taken};
use crate::segments::{self, CleanerConfig, SegmentTable};
use crate::settings::CheckpointConfig;
use crate::tree;
use crate::types::{Epoch, SegmentId, TreeRoot};

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether log batches are big or old enough to checkpoint. Counts the
/// batches' bytes, as backpressure does: their merged size stays small
/// when they overwrite the same keys, and would never come due while
/// backpressure holds writes back.
pub fn due(cfg: &CheckpointConfig, tail: &Tail, now_ms: u64) -> bool {
    tail.retained_bytes() >= cfg.bytes
        || tail
            .since_ms
            .is_some_and(|t| now_ms >= t.saturating_add(ms(cfg.interval)))
}

/// One checkpoint's work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Work {
    /// Log batches to apply; `None` when only purging or cleaning.
    pub taken: Option<Taken>,
    /// Segments whose live nodes move out.
    pub victims: BTreeSet<SegmentId>,
}

/// What a checkpoint at `now` would do, if anything: the oldest batches up
/// to `max_bytes`, segments worth cleaning, or tombstones due for purging.
pub fn plan(
    tail: &Tail,
    m: &Manifest,
    max_bytes: usize,
    cleaner: &CleanerConfig,
    purge_after: Duration,
    now: Ts,
) -> Option<Work> {
    let taken = tail.take(max_bytes);
    let victims = segments::pick_victims(&m.segments, cleaner);
    let purge = tree::purge_due(m.tree.and_then(|r| r.purge_at), now, purge_after);
    (taken.is_some() || !victims.is_empty() || purge).then_some(Work { taken, victims })
}

/// What the data plane produced for one checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Applied {
    pub tree: Option<TreeRoot>,
    pub segments: SegmentTable,
    pub dead: Vec<SegmentId>,
    pub segment_watermark: SegmentId,
}

/// The manifest a checkpoint by writer `epoch` publishes after `prev`, its
/// writer's own manifest.
pub fn next_manifest(
    prev: &Manifest,
    epoch: Epoch,
    taken: Option<&Taken>,
    done: Applied,
    now_ms: u64,
) -> Manifest {
    Manifest {
        writer_epoch: epoch,
        writer: prev.writer,
        parent: prev.writer,
        tree: done.tree,
        last_ts: taken
            .iter()
            .flat_map(|t| t.msgs.iter().map(|(_, v)| v.ts))
            .fold(prev.last_ts, Ts::max),
        wal_applied: taken.map_or(prev.wal_applied, |t| t.upto),
        fences: prev
            .fences
            .iter()
            .chain(taken.iter().flat_map(|t| &t.fences))
            .copied()
            .collect(),
        segment_watermark: done.segment_watermark,
        segments: done.segments,
        dead: done.dead,
        created_at_ms: now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Msg, Version, WalSeq};

    #[test]
    fn applied_fences_accumulate() {
        let prev = Manifest {
            fences: [WalSeq(2)].into(),
            wal_applied: WalSeq(4),
            ..Manifest::initial(0)
        };
        let done = Applied {
            tree: None,
            segments: SegmentTable::new(),
            dead: Vec::new(),
            segment_watermark: prev.segment_watermark,
        };
        let version = Version {
            ts: Ts(70),
            msg: Msg::Delete,
        };
        let taken = Taken {
            msgs: vec![(bytes::Bytes::from_static(b"k"), version)],
            bytes: 0,
            upto: WalSeq(9),
            fences: [WalSeq(6)].into(),
            left: false,
        };
        let m = next_manifest(&prev, Epoch(3), Some(&taken), done.clone(), 7);
        assert_eq!(m.wal_applied, WalSeq(9));
        assert_eq!(m.fences, [WalSeq(2), WalSeq(6)].into());
        assert_eq!((m.writer_epoch, m.last_ts), (Epoch(3), Ts(70)));
        let m = next_manifest(&prev, Epoch(3), None, done, 7);
        assert_eq!((m.wal_applied, m.fences), (WalSeq(4), [WalSeq(2)].into()));
        assert_eq!(m.last_ts, Ts::MIN);
    }
}
