//! Runs the model checker with progress reports:
//!     cargo run --release -p sidra-model --example check [faults]      # exhaustive, small scenario
//!     cargo run --release -p sidra-model --example check fence|last-ts|keep-fences
//!         # that protocol rule broken: search must find a violation
use sidra_model::{Config, Rule, model, small, without};
use stateright::report::WriteReporter;
use stateright::{Checker, HasDiscoveries, Model};

fn main() {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut out = std::io::stdout();
    let mut reporter = WriteReporter::new(&mut out);
    let mut broken = |rule| {
        let checker = model(without(rule))
            .checker()
            .threads(threads)
            .finish_when(HasDiscoveries::AnyFailures)
            .spawn_bfs()
            .report(&mut reporter);
        let found = [
            "acknowledged writes stay visible",
            "durable writes stay visible",
            "log order is timestamp order",
        ]
        .iter()
        .any(|p| checker.discovery(p).is_some());
        assert!(found, "no violation without {rule:?}");
    };
    match std::env::args().nth(1).as_deref() {
        Some("fence") => broken(Rule::Fence),
        Some("last-ts") => broken(Rule::LastTs),
        Some("keep-fences") => broken(Rule::KeepFences),
        faults => {
            let max_faults = u8::from(faults == Some("faults"));
            model(Config {
                max_faults,
                ..small()
            })
            .checker()
            .threads(threads)
            .spawn_bfs()
            .report(&mut reporter)
            .assert_properties();
        }
    }
}
