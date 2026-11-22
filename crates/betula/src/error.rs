use std::sync::Arc;

use crate::types::SegmentId;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The operation needs the applier role, which this process doesn't hold
    /// (or just lost to another process).
    #[error("this process is not the applier")]
    NotApplier,
    /// The writer couldn't renew its lease in time, so the applier may already
    /// treat it as gone. Open a new writer.
    #[error("writer lease expired")]
    LeaseExpired,
    /// Another process is writing under this writer id.
    #[error("writer id already in use")]
    WriterIdTaken,
    #[error("object store: {0}")]
    ObjectStore(Arc<object_store::Error>),
    #[error("corrupt object {path}: {reason}")]
    Corrupt { path: String, reason: String },
    /// A segment referenced by a snapshot was garbage collected. The snapshot
    /// was held longer than the GC retention.
    #[error("snapshot expired: segment {0} was garbage collected")]
    SnapshotExpired(SegmentId),
    #[error("database closed")]
    Closed,
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::ObjectStore(Arc::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
