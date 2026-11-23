//! The protocol's decisions as pure functions: no I/O, no clocks (time is an
//! argument), no shared state. Drivers (`db`, `follow`, `gc`) gather inputs,
//! call these, and carry out the result. See `docs/sans-io.md`.

pub mod applier;
pub mod gc;
pub mod tombstones;
