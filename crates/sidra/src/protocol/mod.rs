//! The protocol's decisions and state machines: no I/O, no clocks (time is
//! an argument), no shared state. Drivers (`db`, `follow`, `gc`) gather
//! inputs, call these, and carry out the result. See `docs/sans-io.md`.

pub mod checkpoint;
pub mod fence;
pub mod gc;
pub mod log;
pub mod tail;

// The data the machines work on, for drivers outside the crate (the model).
pub use crate::manifest::Manifest;
pub use crate::types::{Msg, Version};
pub use crate::wal::{FenceAt, WalBatch, WalRecord};
