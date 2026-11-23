//! Exhaustive search: tens of seconds and ~11 GB, so ignored by default:
//!     cargo test --release -p sidra-model -- --ignored
//! `tests/trace.rs` replays the deep scenarios search can't reach.

use sidra_model::{model, small};
use stateright::{Checker, Model};

#[test]
#[ignore]
fn small_scenario_keeps_every_write_visible() {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let checker = model(small()).checker().threads(threads).spawn_bfs().join();
    checker.assert_properties();
}
