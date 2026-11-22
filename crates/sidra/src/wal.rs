//! Per-writer logs and leases.
//!
//! Each writer appends to its own log, `writers/{id}/wal/{seq}`, so writers
//! never coordinate. A writer's lease, `writers/{id}/lease`, says it is alive
//! and gives a floor under every timestamp it may still write. The applier
//! uses the floors to decide when tombstones can be dropped.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::hlc::WriterId;
use crate::store::{CreateOutcome, Store};
use crate::types::Version;

/// Ops in write order. Each op carries its own timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalBatch {
    pub writer: WriterId,
    pub ops: Vec<(Bytes, Version)>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalWrite {
    Written,
    /// Another process is using this writer id.
    Taken,
}

pub async fn write(store: &Store, seq: u64, batch: &WalBatch) -> Result<WalWrite> {
    let data = codec::encode(codec::WAL, batch);
    match store
        .put_create(&store.wal_path(batch.writer, seq), data.clone())
        .await?
    {
        CreateOutcome::Created => Ok(WalWrite::Written),
        CreateOutcome::Exists(existing) if existing == data => Ok(WalWrite::Written),
        CreateOutcome::Exists(_) => Ok(WalWrite::Taken),
    }
}

pub async fn read(store: &Store, writer: WriterId, seq: u64) -> Result<Option<WalBatch>> {
    let path = store.wal_path(writer, seq);
    store
        .get(&path)
        .await?
        .map(|data| codec::decode(codec::WAL, path.as_ref(), &data))
        .transpose()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub writer: WriterId,
    /// Every timestamp the writer has not yet made durable is at least this.
    pub floor_hlc: u64,
    /// Writer clock. The writer stops acknowledging writes before this.
    pub expires_at_ms: u64,
}

pub async fn write_lease(store: &Store, lease: &Lease) -> Result<()> {
    store
        .put(
            &store.lease_path(lease.writer),
            codec::encode(codec::LEASE, lease),
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
