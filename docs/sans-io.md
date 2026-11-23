# Sans-IO protocol core

Goal: the protocol (writers, appliers, followers, GC) is plain Rust state
machines with no I/O, clocks, or tasks. The async runtime and a Stateright
model both drive the same machines, so the model checks the code that runs.

## Split

| Layer | Contents | I/O |
|---|---|---|
| `protocol` (new) | State machines: what to request next, what a response means, when to ack, seal, claim, drop tombstones | none |
| `tree`, `nodes`, `segments`, `codec` | Data plane: Bε-tree rewrite, node cache, segment packing | node loads only, via the driver |
| `db`, `reader`, `follow` (shrinking) | Drivers: run effects on `object_store`, timers, channels, metrics | all |
| `tests/stateright` (new) | Actors wrapping the same machines, an S3 actor, properties | simulated |

## Shape of a machine

Each role is a state type plus a pure step function:

```rust
fn step(state: &mut S, input: Input, now: Clock) -> Vec<Effect>
```

- `Input`: a store response tagged with the request that caused it
  (`Got { req, object }`, `Put { req, outcome }`, `Listed { req, names }`),
  a timer, or an API call (`Write { ops, ack }`, `Close`).
- `Effect`: a store request with an id (`Get`, `PutIfAbsent`, `Put`,
  `List`, `Delete`), `SetTimer`, `Ack`/`Fail` for callers, and
  `Apply { msgs, drop_below, victims }` for the data plane.
- Time is an input (`now`), never read inside. Independent clocks in the
  model are just different `now` values per actor.

Where code drives steps in order (open: lease, then discover, then observe
the bound), states are typestates; where events arrive from many sources
(WAL batches, poll responses), a reducer over an event enum with every
(state, event) pair handled and illegal pairs returning a typed error.

## Machines

1. **Log writer** (hot path): batching, sequence numbers, put-if-absent,
   acknowledgement, `Taken`/sealed handling, backpressure input.
   Constraint: no allocation or extra work per batch beyond today's.
2. **Lease**: renew schedule, floor, stop after a fatal error.
3. **Follower**: manifest probing, discovery, per-writer log reads, seals,
   tracked writers, tails (unapplied entries).
4. **Applier**: claim/standby/release decisions, checkpoint procedure
   (scan leases, pull, seal expired, apply, upload, publish), fencing.
5. **GC**: what to delete given manifests, logs, leases, seals.

## Stateright model

- Actors: N writer processes (each composes machines 1–4), one S3 actor
  (objects, put-if-absent, list, delete; may delay, reorder, fail, or lose
  responses), crash/restart of writer processes.
- Abstract data plane: `Apply` merges messages into a key→version map
  (the tree's contract, already covered by proptests and Kani).
- Properties: acknowledged writes are never lost or hidden (last-writer-wins
  by timestamp over acknowledged writes equals what a fresh reader sees);
  one applier per epoch publishes; a sealed log takes no further batch.
- Replaces `spec/Sidra.tla` once it covers the same and more.

## Phases (each lands separately, perf-checked against the previous)

1. Extract pure decision functions from `db.rs`/`follow.rs`/`gc.rs` into
   `protocol` (claim decision, floor and bound, seal targets, frontier,
   manifest assembly, GC plan). No behavior change.
2. Log writer and lease as machines; drivers interpret effects. Bench the
   write path (load/update throughput and p99) against phase 1.
3. Follower and applier as machines.
4. Stateright model and properties; CI runs a bounded check.
5. Retire the TLA+ model.
