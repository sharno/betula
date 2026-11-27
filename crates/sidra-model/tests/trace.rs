//! The scenarios behind each protocol rule as replayed traces: with the
//! rule, every property holds; without it, a write goes missing. Process 0
//! opens the first writer, process 1 the one that fences it; see `small`.

use sidra_model::{
    Checked, Fate, Path, Rule, Sys, Timer, acknowledged_writes_stay_visible,
    durable_writes_stay_visible, model, small, without,
};

const OLD: usize = 0;
const NEW: usize = 1;

fn step(m: &Checked, s: Sys, proc: usize, timer: Timer) -> Sys {
    let s = m.fire(s, proc, timer);
    m.drain(s, proc)
}

/// The old writer's delete is in flight while the new writer opens, puts,
/// checkpoints, and GC collects the applied log. Then the delete lands.
fn zombie(m: &Checked) -> Sys {
    let mut s = m.initial();
    s = step(m, s, OLD, Timer::Start);
    s = step(m, s, OLD, Timer::Clock);
    s = m.fire(s, OLD, Timer::Write);
    s = step(m, s, NEW, Timer::Start);
    assert!(s.is_open(NEW));
    s = step(m, s, NEW, Timer::Write);
    s = step(m, s, NEW, Timer::Checkpoint);
    s = m.gc(s);
    m.deliver(s, OLD, Fate::Ok)
}

#[test]
fn a_delayed_put_lands_on_the_fence_even_after_gc() {
    let m = model(small());
    let s = zombie(&m);
    assert!(s.is_stopped(OLD));
    assert!(
        s.acked(OLD).is_empty(),
        "the late put must not be acknowledged"
    );
    assert!(acknowledged_writes_stay_visible(&m, &s));
}

#[test]
fn without_kept_fences_a_delayed_put_lands_where_no_reader_looks() {
    let m = model(without(Rule::KeepFences));
    let s = zombie(&m);
    assert_eq!(s.acked(OLD).len(), 1);
    assert!(!acknowledged_writes_stay_visible(&m, &s));
}

#[test]
fn without_a_fence_a_delayed_put_lands_where_no_reader_looks() {
    let m = model(without(Rule::Fence));
    let s = zombie(&m);
    assert_eq!(s.acked(OLD).len(), 1);
    assert!(!acknowledged_writes_stay_visible(&m, &s));
}

/// The old writer deletes `k` at clock 2 and checkpoints, which drops the
/// tombstone. The new writer opens at clock 1 and puts `k`.
fn lagging(m: &Checked) -> Sys {
    let mut s = m.initial();
    s = step(m, s, OLD, Timer::Start);
    s = step(m, s, OLD, Timer::Clock);
    s = step(m, s, OLD, Timer::Write);
    s = step(m, s, OLD, Timer::Checkpoint);
    s = step(m, s, NEW, Timer::Start);
    step(m, s, NEW, Timer::Write)
}

#[test]
fn a_lagging_new_writer_stamps_past_the_applied_log() {
    let m = model(small());
    let s = lagging(&m);
    assert_eq!(s.acked(NEW).len(), 1);
    assert!(s.acked(NEW)[0].1.ts > s.acked(OLD)[0].1.ts);
    assert!(acknowledged_writes_stay_visible(&m, &s));
}

#[test]
fn without_last_ts_a_lagging_writer_stamps_before_a_dropped_delete() {
    let m = model(without(Rule::LastTs));
    let s = lagging(&m);
    assert!(s.acked(NEW)[0].1.ts < s.acked(OLD)[0].1.ts);
    assert!(!acknowledged_writes_stay_visible(&m, &s));
}

/// The old writer's checkpoint is about to publish manifest 2 when the new
/// writer claims 2, applies the log in two checkpoints, and GC collects
/// every manifest below the newest. Then the old writer's publish lands in
/// the collected id.
fn stalled_checkpoint(m: &Checked) -> Sys {
    let mut s = m.initial();
    s = step(m, s, OLD, Timer::Start);
    s = step(m, s, OLD, Timer::Write);
    s = m.fire(s, OLD, Timer::Checkpoint);
    s = m.deliver(s, OLD, Fate::Ok); // loads the tree
    s = m.deliver(s, OLD, Fate::Ok); // puts its segment
    s = step(m, s, NEW, Timer::Start);
    s = step(m, s, NEW, Timer::Write);
    s = step(m, s, NEW, Timer::Checkpoint);
    s = step(m, s, NEW, Timer::Checkpoint);
    s = m.gc(s);
    assert!(!s.store.contains_key(&Path::Manifest(2)));
    m.drain(s, OLD)
}

#[test]
fn a_checkpoint_stalled_past_gc_lands_as_a_stray_and_stops() {
    let m = model(small());
    let s = stalled_checkpoint(&m);
    assert!(s.is_stopped(OLD));
    assert!(s.store.contains_key(&Path::Manifest(2)), "the stray landed");
    let s = m.gc(s);
    assert!(
        !s.store.contains_key(&Path::Manifest(2)),
        "GC deleted the stray"
    );
    assert!(acknowledged_writes_stay_visible(&m, &s));
    assert!(durable_writes_stay_visible(&m, &s));
}
