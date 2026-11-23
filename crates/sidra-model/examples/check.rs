//! Runs the model checker with progress reports:
//!     cargo run --release -p sidra-model --example check            # the protocol: exhaustive
//!     cargo run --release -p sidra-model --example check no-bound   # the bug, by random walks
use sidra_model::{late_writer, model};
use stateright::report::WriteReporter;
use stateright::{Checker, HasDiscoveries, Model, UniformChooser};

fn main() {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut out = std::io::stdout();
    let mut reporter = WriteReporter::new(&mut out);
    match std::env::args().nth(1).as_deref() {
        // The bug is ~60 steps deep: random walks reach it without storing
        // the whole space.
        Some("no-bound") => {
            let checker = model(late_writer(false))
                .checker()
                .threads(threads)
                .finish_when(HasDiscoveries::AnyFailures)
                .target_max_depth(120)
                .spawn_simulation(0, UniformChooser)
                .report(&mut reporter);
            assert!(
                checker
                    .discovery("acknowledged writes stay visible")
                    .is_some()
            );
        }
        _ => {
            let checker = model(late_writer(true))
                .checker()
                .threads(threads)
                .spawn_bfs()
                .report(&mut reporter);
            checker.assert_properties();
        }
    }
}
