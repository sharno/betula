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
    /// A log write still failed after retries, so whether it landed is
    /// unknown and the writer can't continue its log. Open a new writer;
    /// writes not acknowledged may or may not be durable.
    #[error("log write failed: {0}")]
    LogWriteFailed(Arc<Error>),
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
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// What an error means for the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The object store failed after retries. The operation may succeed if
    /// repeated.
    Unavailable,
    /// This writer can no longer write (lease expired, writer id taken, or a
    /// log write failed). Open a new one.
    WriterLost,
    /// The operation needs the applier role, which this process doesn't hold.
    NotApplier,
    /// The handle was closed.
    Closed,
    /// A read referred to data deleted by GC: the snapshot was held past the
    /// retention.
    SnapshotExpired,
    /// Stored data failed validation.
    Corrupt,
    /// The settings or URL are invalid.
    InvalidConfig,
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::ObjectStore(_) => ErrorKind::Unavailable,
            Error::LeaseExpired | Error::WriterIdTaken | Error::LogWriteFailed(_) => {
                ErrorKind::WriterLost
            }
            Error::NotApplier => ErrorKind::NotApplier,
            Error::Closed => ErrorKind::Closed,
            Error::SnapshotExpired(_) => ErrorKind::SnapshotExpired,
            Error::Corrupt { .. } => ErrorKind::Corrupt,
            Error::InvalidConfig(_) => ErrorKind::InvalidConfig,
        }
    }

    /// Whether repeating the operation can succeed.
    pub fn is_retryable(&self) -> bool {
        self.kind() == ErrorKind::Unavailable
    }
}

impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        Error::ObjectStore(Arc::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
