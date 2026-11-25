//! sidra: a copy-on-write Bε-tree key-value store on object storage, with
//! any number of concurrent writers.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> sidra::Result<()> {
//! use sidra::{Db, ReadOptions, Freshness, WriteBatch};
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
//! // See writes other processes acknowledged before this read.
//! let latest = ReadOptions { freshness: Freshness::Latest };
//! let _ = db.get_with("user:1", latest).await?;
//!
//! let users = db.scan("user:".."user;").collect().await?;
//! assert_eq!(users.len(), 2);
//! db.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! How it works:
//! - Each writer appends timestamped writes (hybrid logical clock) to its own
//!   log on object storage. Writers never coordinate, and a write is
//!   acknowledged once its log object is durable.
//! - One process at a time holds the applier role. It folds every writer's
//!   log into the tree in batches, writing changed nodes copy-on-write into
//!   segment objects, and publishes a new manifest with put-if-absent. Any
//!   writer takes the role over if entries go unapplied.
//! - Versions merge newest-wins, so the order of application doesn't matter.
//!   Values are per-key last-writer-wins.
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
mod logs;
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

pub use admin::{Admin, ManifestInfo, WriterInfo};
pub use codec::Compression;
pub use db::{
    Audit, Clock, Db, DbBuilder, Durability, Freshness, Metrics, ReadOptions, SystemClock,
    WriteBatch, WriteOptions,
};
pub use error::{Error, ErrorKind, Result};
pub use gc::{GcConfig, GcStats};
pub use hedge::{Fraction, Hedge, HedgeConfig};
pub use hlc::{Ts, WriterId};
pub use location::parse as parse_url;
pub use logs::{LogHome, Region};
pub use nodes::{CacheConfig, CacheStats, Warm};
pub use object_store;
pub use reader::{DbReader, DbReaderBuilder};
pub use segments::{CleanerConfig, SegmentKind, SegmentTable, SegmentUsage};
pub use settings::{
    ApplierRole, CheckpointConfig, Follow, Gc, LeaseConfig, PollConfig, ReaderSettings, Settings,
    WalConfig,
};
pub use snapshot::DbIterator;
pub use store::RequestStats;
pub use tree::{TreeConfig, TreeShape};
pub use types::{Epoch, KeyRange, ManifestId, NodePtr, SegmentId, TreeRoot};
