//! sidra: a copy-on-write Bε-tree key-value store on object storage, with
//! one writer and any number of readers.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> sidra::Result<()> {
//! use sidra::{Db, WriteBatch};
//!
//! // `s3://bucket/path`, `gs://…`, `az://…`, `file:///…` or `memory:///`.
//! // Credentials come from the usual AWS_* / GOOGLE_* / AZURE_* variables,
//! // settings from SIDRA_CONFIG and SIDRA_* (see `Settings`).
//! let db = Db::open("memory:///quickstart").await?;
//!
//! db.put("user:1", "ada").await?;
//! let mut batch = WriteBatch::new();
//! batch.put("user:2", "grace").delete("user:3");
//! db.write(batch).await?; // atomic
//!
//! assert_eq!(db.get("user:1").await?.as_deref(), Some(&b"ada"[..]));
//!
//! let users = db.scan("user:".."user;").collect().await?;
//! assert_eq!(users.len(), 2);
//! db.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! How it works:
//! - One writer at a time. It appends timestamped writes to a log on object
//!   storage and acknowledges a write once its log object is durable.
//!   Opening a writer fences the previous one.
//! - The writer folds the log into the tree in checkpoints, writing changed
//!   nodes copy-on-write into segment objects, and publishes a new manifest
//!   with put-if-absent.
//! - Readers ([`DbReader`]) follow the manifest and the log past it.
//! - Internal nodes buffer messages per child as append-only runs (the Bε
//!   part). They are cached and loaded on open, so a cold point read costs
//!   one GET.

mod admin;
mod codec;
mod db;
mod error;
mod follow;
mod gc;
mod hedge;
mod hlc;
mod location;
mod manifest;
mod memtable;
mod node;
mod nodes;
#[cfg(kani)]
mod proofs;
/// The protocol's state machines and decisions, for model checking
/// (`sidra-model`). Not a stable API.
#[doc(hidden)]
pub mod protocol;
mod reader;
mod retry;
mod segments;
mod settings;
mod snapshot;
mod store;
mod tree;
mod types;
mod wal;

pub use admin::{Admin, LogInfo, ManifestInfo};
pub use codec::Compression;
pub use db::{
    Audit, Clock, Db, DbBuilder, Durability, Metrics, SystemClock, WriteBatch, WriteOptions,
};
pub use error::{Error, ErrorKind, Result};
pub use gc::{GcConfig, GcStats};
pub use hedge::{Fraction, Hedge, HedgeConfig};
pub use hlc::Ts;
pub use location::parse as parse_url;
pub use nodes::{CacheConfig, CacheStats, Warm};
pub use object_store;
pub use reader::{DbReader, DbReaderBuilder, Freshness, ReadOptions};
pub use segments::{CleanerConfig, SegmentKind, SegmentTable, SegmentUsage};
pub use settings::{CheckpointConfig, Gc, ReaderSettings, Settings, WalConfig};
pub use snapshot::DbIterator;
pub use store::RequestStats;
pub use tree::{TreeConfig, TreeShape};
pub use types::{Epoch, KeyRange, ManifestId, NodePtr, SegmentId, TreeRoot, WalSeq, WriterId};
