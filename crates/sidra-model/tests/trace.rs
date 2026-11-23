//! The tombstone-bound bug as one replayed trace (search rarely reaches an
//! interleaving this specific). Processes: 0 applier, 1 deleter, 2 late
//! writer; see `late_writer`.

use sidra_model::{Checked, Sys, Timer, acknowledged_writes_stay_visible, late_writer, model};

const APPLIER: usize = 0;
const DELETER: usize = 1;
const LATE: usize = 2;

fn step(m: &Checked, s: Sys, proc: usize, timer: Timer) -> Sys {
    let s = m.fire(s, proc, timer);
    m.drain(s, proc)
}

fn trace(observe_bound: bool) -> (Checked, Sys) {
    let m = model(late_writer(observe_bound));
    let mut s = m.initial();
    // The deleter opens at clock 1 and deletes `k` at clock 2.
    s = step(&m, s, DELETER, Timer::Start);
    s = step(&m, s, DELETER, Timer::Clock);
    s = step(&m, s, DELETER, Timer::Write);
    // At clock 3 its renewed lease's floor passes the delete.
    s = step(&m, s, DELETER, Timer::Clock);
    s = step(&m, s, DELETER, Timer::Lease);
    // The applier, at clock 3, applies the delete and publishes bound 3.
    s = step(&m, s, APPLIER, Timer::Clock);
    s = step(&m, s, APPLIER, Timer::Clock);
    s = step(&m, s, APPLIER, Timer::Checkpoint);
    // The next write gives the next checkpoint work; it drops `k`'s
    // tombstone, since no leased writer can still write below 3.
    s = step(&m, s, DELETER, Timer::Write);
    s = step(&m, s, APPLIER, Timer::Checkpoint);
    // Only now does the late writer open, at clock 1, and put `k`.
    s = step(&m, s, LATE, Timer::Start);
    s = step(&m, s, LATE, Timer::Write);
    (m, s)
}

#[test]
fn without_the_bound_the_late_put_resurrects_a_deleted_key() {
    let (m, s) = trace(false);
    assert!(!acknowledged_writes_stay_visible(&m, &s));
}

#[test]
fn with_the_bound_the_late_put_is_newer_than_the_delete() {
    let (m, s) = trace(true);
    assert!(acknowledged_writes_stay_visible(&m, &s));
}
