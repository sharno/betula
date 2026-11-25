//! The write-ahead log: one object per slot, `wal/{seq}`, written with
//! put-if-absent at consecutive slots. A writer never skips one, so the log
//! has no gaps.
//!
//! A new writer fences the previous one by putting a fence record in the
//! first empty slot. The previous writer's next put lands on that slot,
//! finds it taken, and stops; nothing it writes later can be acknowledged.
//! GC keeps every fence (see `Manifest::fences`).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::store::{CreateOutcome, Store};
use crate::types::{Epoch, Version, WalSeq};

/// Ops in write order, each with its own timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalBatch {
    /// The writer generation that wrote it.
    pub epoch: Epoch,
    pub ops: Vec<(Bytes, Version)>,
}

/// One log object.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WalRecord {
    Batch(WalBatch),
    /// Writer generation `0` took over here: writers before it can't write
    /// at or after this slot.
    Fence(Epoch),
}

/// Encodes like `WalRecord::Batch` (the first variant, so the same tag)
/// without cloning the batch.
#[derive(Serialize)]
enum RecordRef<'a> {
    Batch(&'a WalBatch),
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalWrite {
    Written,
    /// Something else holds the slot: a newer writer fenced this one.
    Fenced,
}

fn decode(store: &Store, seq: WalSeq, data: &[u8]) -> Result<WalRecord> {
    codec::decode(codec::WAL, store.wal_path(seq).as_ref(), data)
}

pub async fn write(
    store: &Store,
    seq: WalSeq,
    batch: &WalBatch,
    compression: codec::Compression,
) -> Result<WalWrite> {
    let data = codec::encode(codec::WAL, &RecordRef::Batch(batch), compression);
    match store.put_create(&store.wal_path(seq), data.clone()).await? {
        CreateOutcome::Created => Ok(WalWrite::Written),
        // Our own earlier attempt whose response was lost.
        CreateOutcome::Exists(existing) if existing == data => Ok(WalWrite::Written),
        CreateOutcome::Exists(_) => Ok(WalWrite::Fenced),
    }
}

/// What a fence attempt at one slot found.
#[derive(Debug, PartialEq, Eq)]
pub enum FenceAt {
    /// Our fence is in place.
    Placed,
    /// Something got there first.
    Taken(WalRecord),
}

/// Puts a fence for `epoch` at `seq` unless something is there already.
pub async fn fence(store: &Store, seq: WalSeq, epoch: Epoch) -> Result<FenceAt> {
    let data = codec::encode(
        codec::WAL,
        &WalRecord::Fence(epoch),
        codec::Compression::None,
    );
    match store.put_create(&store.wal_path(seq), data.clone()).await? {
        CreateOutcome::Created => Ok(FenceAt::Placed),
        CreateOutcome::Exists(existing) if existing == data => Ok(FenceAt::Placed),
        CreateOutcome::Exists(existing) => Ok(FenceAt::Taken(decode(store, seq, &existing)?)),
    }
}

pub async fn read(store: &Store, seq: WalSeq) -> Result<Option<WalRecord>> {
    store
        .get(&store.wal_path(seq))
        .await?
        .map(|data| decode(store, seq, &data))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Ts;
    use crate::types::Msg;

    #[test]
    fn records_decode_as_encoded() {
        let batch = WalBatch {
            epoch: Epoch(3),
            ops: vec![(
                Bytes::from_static(b"k"),
                Version {
                    ts: Ts(1),
                    msg: Msg::Delete,
                },
            )],
        };
        let decode = |data: Bytes| codec::decode::<WalRecord>(codec::WAL, "t", &data).unwrap();
        let lz4 = codec::Compression::Lz4;
        assert_eq!(
            decode(codec::encode(codec::WAL, &RecordRef::Batch(&batch), lz4)),
            WalRecord::Batch(batch.clone())
        );
        let fence = WalRecord::Fence(Epoch(4));
        assert_eq!(decode(codec::encode(codec::WAL, &fence, lz4)), fence);
    }
}
