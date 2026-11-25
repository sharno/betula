use std::sync::Arc;

use crate::types::SegmentId;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// A newer writer opened the database, so this one can't write or
    /// checkpoint anymore. Writes it didn't acknowledge may or may not be
    /// durable; the newer writer applies those that are.
    #[error("fenced by a newer writer")]
    Fenced,
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
    /// This writer can no longer write (fenced by a newer one, or a log
    /// write failed). Open a new one if this process should still write.
    WriterLost,
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
            Error::Fenced | Error::LogWriteFailed(_) => ErrorKind::WriterLost,
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
