//! Exhaustive search, release builds only (debug is far slower):
//!     cargo test --release -p sidra-model -- --ignored
//! `tests/trace.rs` replays the scenarios behind each protocol rule.

use sidra_model::{Config, model, small};
use stateright::{Checker, Model};

fn check(cfg: Config) {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let checker = model(cfg).checker().threads(threads).spawn_bfs().join();
    checker.assert_properties();
}

#[test]
#[ignore]
fn small_scenario_keeps_every_write_visible() {
    check(small());
}

#[test]
#[ignore]
fn small_scenario_with_a_fault() {
    check(Config {
        max_faults: 1,
        ..small()
    });
}
