//! betula: a copy-on-write Bε-tree on object storage.
//!
//! - Writes go to a WAL on object storage, then a memtable, then are applied
//!   to the tree in batches. Each batch writes new nodes (copy-on-write) and
//!   publishes a new manifest with put-if-absent.
//! - Internal nodes buffer messages for their children (the Bε part), so a
//!   batch only rewrites the root-to-leaf paths whose buffers overflow.
//! - Internal nodes are cached separately and warmed on open, so a cold point
//!   read costs one GET.

mod codec;
mod db;
mod error;
mod gc;
mod manifest;
mod memtable;
mod node;
mod nodes;
mod reader;
mod retry;
mod snapshot;
mod store;
mod tree;
mod types;
mod wal;

pub use db::{Clock, Db, DbConfig, DbStats, Durability, SystemClock, WriteBatch};
pub use error::{Error, Result};
pub use gc::{GcConfig, GcStats};
pub use nodes::CacheConfig;
pub use reader::{DbReader, ReaderConfig};
pub use snapshot::DbIterator;
pub use tree::{TreeConfig, TreeShape};
pub use types::{Epoch, KeyRange, ManifestId, NodeId, TreeRoot};
