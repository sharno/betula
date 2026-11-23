//! Full model checks: minutes and tens of GB of RAM each, so ignored by
//! default. Run with:
//!     cargo test --release -p sidra-model -- --ignored --test-threads 1
//! or watch progress with `cargo run --release -p sidra-model --example check`.

use sidra_model::{late_writer, model};
use stateright::{Checker, Model};

fn threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

#[test]
#[ignore]
fn late_writer_cannot_undercut_a_dropped_tombstone() {
    let checker = model(late_writer(true))
        .checker()
        .threads(threads())
        .spawn_bfs()
        .join();
    checker.assert_properties();
}

// Open: no counterexample yet (exhaustive search doesn't fit in memory at
// this depth, and 3.4B random-walk states found none). Until the model
// reproduces it, the tombstone-bound rule rests on the TLA+ model and
// `lagging_new_writer_cannot_undercut_a_dropped_tombstone` in sidra's tests.
#[test]
#[ignore]
fn without_the_bound_a_late_writer_resurrects_a_key() {
    let checker = model(late_writer(false))
        .checker()
        .finish_when(stateright::HasDiscoveries::AnyFailures)
        .threads(threads())
        .spawn_bfs()
        .join();
    assert!(
        checker
            .discovery("acknowledged writes stay visible")
            .is_some(),
        "the model should find the bug the tombstone bound fixes"
    );
}
