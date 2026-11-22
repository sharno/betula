//! betula: a copy-on-write Bε-tree on object storage with any number of
//! concurrent writers.
//!
//! - Each writer appends timestamped writes (hybrid logical clock) to its own
//!   log on object storage. Writers never coordinate.
//! - One process at a time holds the applier role. It folds every writer's
//!   log into the tree in batches, writing changed nodes into one segment
//!   object per batch (copy-on-write), and publishes a new manifest with
//!   put-if-absent. Any writer takes the role over if entries go unapplied.
//! - Versions merge newest-wins, so order of application doesn't matter.
//! - Internal nodes buffer messages for their children (the Bε part), are
//!   cached separately and warmed on open, so a cold point read costs one GET.

mod codec;
mod db;
mod error;
mod follow;
mod gc;
mod hlc;
mod manifest;
mod memtable;
mod node;
mod nodes;
#[cfg(kani)]
mod proofs;
mod reader;
mod retry;
mod segments;
mod snapshot;
mod store;
mod tree;
mod types;
mod wal;

pub use db::{
    ApplierRole, Clock, Db, DbConfig, DbStats, Durability, LeaseConfig, SystemClock, WriteBatch,
};
pub use error::{Error, Result};
pub use gc::{GcConfig, GcStats};
pub use hlc::{Ts, WriterId};
pub use nodes::CacheConfig;
pub use reader::{DbReader, ReaderConfig};
pub use segments::{CleanerConfig, SegmentTable, SegmentUsage};
pub use snapshot::DbIterator;
pub use tree::{TreeConfig, TreeShape};
pub use types::{Epoch, KeyRange, ManifestId, NodePtr, SegmentId, TreeRoot};
