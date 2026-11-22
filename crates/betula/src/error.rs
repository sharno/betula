use std::sync::Arc;

use crate::types::SegmentId;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// A newer writer opened the database. This handle can no longer write.
    #[error("writer fenced by a newer writer")]
    Fenced,
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
