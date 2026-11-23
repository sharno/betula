//! Per-writer logs and leases.
//!
//! Each writer appends to its own log, `wal/{id}/{seq}`, so writers never
//! coordinate. Log objects are put-if-absent at consecutive sequence numbers,
//! and a writer never skips one. So a seal record put at a writer's next
//! sequence number ends its log for good: no batch can land at or after it,
//! and the writer's own put there fails. The applier seals a writer it
//! presumes dead before it stops counting it in the tombstone floor; a
//! closing writer seals its own log. Seals are never deleted, so a sealed
//! writer id can't write again however late its requests arrive.
//!
//! A writer's lease, `writers/{id}/lease`, says it is alive and gives a floor
//! under every timestamp it may still write. Expiry is only a hint for when
//! to seal; safety never depends on clocks.
//!
//! Leases always live in the home store; a log may live in a regional store
//! (see `logs`).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::hlc::WriterId;
use crate::logs::LogHome;
use crate::settings::Candidacy;
use crate::store::{CreateOutcome, Store};
use crate::types::Version;

/// Ops in write order. Each op carries its own timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalBatch {
    pub writer: WriterId,
    pub ops: Vec<(Bytes, Version)>,
}

/// One log object.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WalRecord {
    Batch(WalBatch),
    /// The end of the log: nothing exists or can be written after it.
    Seal,
}

/// Encodes like `WalRecord` without cloning the batch.
#[derive(Serialize)]
enum RecordRef<'a> {
    Batch(&'a WalBatch),
    Seal,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalWrite {
    Written,
    /// The log was sealed at this sequence number.
    Sealed,
    /// Another process is using this writer id.
    Taken,
}

fn decode(store: &Store, writer: WriterId, seq: u64, data: &[u8]) -> Result<WalRecord> {
    codec::decode(codec::WAL, store.wal_path(writer, seq).as_ref(), data)
}

pub async fn write(
    store: &Store,
    seq: u64,
    batch: &WalBatch,
    compression: codec::Compression,
) -> Result<WalWrite> {
    let data = codec::encode(codec::WAL, &RecordRef::Batch(batch), compression);
    match store
        .put_create(&store.wal_path(batch.writer, seq), data.clone())
        .await?
    {
        CreateOutcome::Created => Ok(WalWrite::Written),
        CreateOutcome::Exists(existing) if existing == data => Ok(WalWrite::Written),
        CreateOutcome::Exists(existing) => match decode(store, batch.writer, seq, &existing)? {
            WalRecord::Seal => Ok(WalWrite::Sealed),
            WalRecord::Batch(_) => Ok(WalWrite::Taken),
        },
    }
}

/// What a seal attempt at one sequence number found.
#[derive(Debug, PartialEq, Eq)]
pub enum SealAt {
    /// The seal is in place (ours, or someone else's).
    Sealed,
    /// A batch got there first; the log goes on after it.
    Batch(WalBatch),
}

/// Puts a seal at `seq` unless something is there already.
pub async fn seal(store: &Store, writer: WriterId, seq: u64) -> Result<SealAt> {
    let data = codec::encode(codec::WAL, &RecordRef::Seal, codec::Compression::None);
    match store.put_create(&store.wal_path(writer, seq), data).await? {
        CreateOutcome::Created => Ok(SealAt::Sealed),
        CreateOutcome::Exists(existing) => match decode(store, writer, seq, &existing)? {
            WalRecord::Seal => Ok(SealAt::Sealed),
            WalRecord::Batch(b) => Ok(SealAt::Batch(b)),
        },
    }
}

pub async fn read(store: &Store, writer: WriterId, seq: u64) -> Result<Option<WalRecord>> {
    store
        .get(&store.wal_path(writer, seq))
        .await?
        .map(|data| decode(store, writer, seq, &data))
        .transpose()
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Lease {
    pub writer: WriterId,
    /// Where this writer's log lives. Fixed for the writer's lifetime.
    pub log: LogHome,
    /// A standby applier hands the role back while a primary is alive.
    pub candidacy: Candidacy,
    /// Every timestamp the writer has not yet made durable is at least this.
    pub floor_hlc: u64,
    /// Writer clock. Past it, the applier seals the writer's log.
    pub expires_at_ms: u64,
}

pub async fn write_lease(store: &Store, lease: &Lease) -> Result<()> {
    store
        .put(
            &store.lease_path(lease.writer),
            codec::encode(codec::LEASE, lease, codec::Compression::None),
        )
        .await
}

pub async fn read_lease(store: &Store, writer: WriterId) -> Result<Option<Lease>> {
    let path = store.lease_path(writer);
    store
        .get(&path)
        .await?
        .map(|data| codec::decode(codec::LEASE, path.as_ref(), &data))
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
            writer: WriterId(7),
            ops: vec![(
                Bytes::from_static(b"k"),
                Version {
                    ts: Ts {
                        hlc: 1,
                        writer: WriterId(7),
                    },
                    msg: Msg::Delete,
                },
            )],
        };
        let decode = |r: &RecordRef| {
            let data = codec::encode(codec::WAL, r, codec::Compression::Lz4);
            codec::decode::<WalRecord>(codec::WAL, "t", &data).unwrap()
        };
        assert_eq!(
            decode(&RecordRef::Batch(&batch)),
            WalRecord::Batch(batch.clone())
        );
        assert_eq!(decode(&RecordRef::Seal), WalRecord::Seal);
    }
}
