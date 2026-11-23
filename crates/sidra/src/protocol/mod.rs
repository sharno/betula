//! The protocol's decisions as pure functions: no I/O, no clocks (time is an
//! argument), no shared state. Drivers (`db`, `follow`, `gc`) gather inputs,
//! call these, and carry out the result. See `docs/sans-io.md`.

pub mod applier;
pub mod checkpoint;
pub mod follower;
pub mod gc;
pub mod log;
pub mod tombstones;

// The data the machines work on, for drivers outside the crate (the model).
pub use crate::manifest::{Frontier, Manifest};
pub use crate::settings::Candidacy;
pub use crate::types::{Msg, Version};
pub use crate::wal::{Lease, WalBatch, WalRecord};
