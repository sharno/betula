use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::store::{CreateOutcome, Store};
use crate::types::{Epoch, Msg, WalId};

/// Ops in write order; a later op on the same key wins. A batch with no ops
/// is a fence written by a new writer to claim the next WAL id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalBatch {
    pub epoch: Epoch,
    pub ops: Vec<(Bytes, Msg)>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WalWrite {
    Written,
    /// Another writer owns this id.
    Taken,
}

pub async fn write(store: &Store, id: WalId, batch: &WalBatch) -> Result<WalWrite> {
    let data = codec::encode(codec::WAL, batch);
    match store.put_create(&store.wal_path(id), data.clone()).await? {
        CreateOutcome::Created => Ok(WalWrite::Written),
        CreateOutcome::Exists(existing) if existing == data => Ok(WalWrite::Written),
        CreateOutcome::Exists(_) => Ok(WalWrite::Taken),
    }
}

pub async fn read(store: &Store, id: WalId) -> Result<Option<WalBatch>> {
    let path = store.wal_path(id);
    store
        .get(&path)
        .await?
        .map(|data| codec::decode(codec::WAL, path.as_ref(), &data))
        .transpose()
}
