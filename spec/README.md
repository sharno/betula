# Formal verification

Two layers:

- **`Betula.tla`**: a TLA+ model of the multi-writer protocol, model-checked
  with [everystate](https://crates.io/crates/everystate), a TLC-compatible
  checker that needs no Java.
- **`crates/betula/src/proofs.rs`**: [Kani](https://model-checking.github.io/kani/)
  bounded proofs of the core pure functions in the Rust code itself.

## Protocol model (TLA+)

The model covers one key and these actors:
- writers that start late, stamp writes, flush batches to their own logs,
  renew leases, and self-fence (losing unacknowledged writes);
- appliers that read leases, pull logs, and publish a new tree with
  put-if-absent on the next manifest version;
- GC of log entries the manifest already holds.

The tree is a buffer level over a leaf level. Tombstones are dropped below
the lease floor when a leaf is rewritten (a flush, or a purge or relocation).

| Invariant | Meaning |
|---|---|
| `Correct` | A reader (tree plus unapplied log entries) sees exactly the newest durable write: none lost, none resurrected |
| `TreeFromLogs` | The tree never holds a version that isn't durable |

Each switch turns one protocol rule off. The checker finds a counterexample
for each, so the invariants are strong enough to catch these bugs:

| Config | Rule disabled | Result |
|---|---|---|
| `Betula.cfg` | none (2 writers, 1 applier) | pass, 22.5M distinct states |
| `TwoAppliers.cfg` | none (2 appliers racing) | pass, 146.9M distinct states |
| `BugDiscovery.cfg` | the checkpoint pulls every writer whose lease it read | violation in 14 steps |
| `BugCarryDown.cfg` | a leaf rewrite carries the buffered versions above it down | violation in 16 steps |
| `BugUnfenced.cfg` | manifest publish is put-if-absent | violation in 11 steps |

`BugDiscovery` reproduced a real bug that randomized testing found once and
then couldn't reproduce. The applier computed the floor from every writer's
lease but pulled logs only from writers it already knew. A new writer's older
`put` then arrived after a newer tombstone for the same key had been dropped,
and the key came back.

```sh
cargo install everystate --locked
./spec/check.sh
```

## Code proofs (Kani)

Each harness is checked over every input up to small bounds:

| Harness | Property |
|---|---|
| `merge_newest_keeps_newest_per_key` | Merge output is sorted, keeps each key once with its newest version, and invents nothing |
| `merge_newest_is_commutative` / `_is_associative` | Applying writes in any order or grouping gives the same result |
| `chunk_by_bytes_partitions_in_order` | Leaf splits keep every entry in order, have no empty leaves, and meet the size target |
| `group_starts_respect_fanout` | Internal-node splits cover every child and respect the fanout |
| `routing_is_consistent` | A key goes to the child that covers it; range scans start and stop at the right children |
| `splice_keeps_node_shape` | Child replacement and removal keep `children = pivots + 1`, with pivots sorted |
| `hlc_is_strictly_monotonic` | The hybrid clock never repeats or goes back, and orders after anything it observed |
| `account_tracks_live_bytes` | Segment live bytes drop by exactly what was freed; dead exactly when zero |
| `victims_respect_threshold_and_budget` | The cleaner picks only segments below the threshold, within its budget |

```sh
cargo install --locked kani-verifier && cargo kani setup
cd crates/betula && cargo kani
```

## Not covered

- Multiple keys and scans are covered by the model-based proptest
  (`tests/model.rs`), not by these proofs.
- Segment GC and the cleaner's relocation are checked only by the accounting
  invariant in the proptest.
- The model assumes lease expiry is honored. Its safety rests on clock skew
  between processes staying within the lease margin.
