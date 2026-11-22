# Formal verification

Two layers:

- **`Sidra.tla`**: a TLA+ model of the multi-writer protocol, model-checked
  with [everystate](https://crates.io/crates/everystate), a TLC-compatible
  checker that needs no Java.
- **`crates/sidra/src/proofs.rs`**: [Kani](https://model-checking.github.io/kani/)
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
| `Sidra.cfg` | none (2 writers, 1 applier) | pass, 22.5M distinct states |
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

Each harness checks every value of its inputs at the listed sizes. Sizes are
concrete per harness (CBMC's formulas blow up with symbolic lengths or 64-bit
division); contents are symbolic.

| Harnesses | Property | Sizes | Result |
|---|---|---|---|
| `merge_newest_*` (4) | Merge output is sorted, keeps each key once with its newest version, and invents nothing | runs of 1–2 entries | ✅ |
| `merge_commutes_*` (3), `merge_associates_*` (3) | Applying writes in any order or grouping gives the same result | runs of 1–2 | ✅ |
| `chunks_*` (3) | Leaf splits keep every entry in order, have no empty leaves, and meet the size target | 1–3 entries | ✅ |
| `group_starts_*` (6) | Internal-node splits cover every child and respect the fanout | n ∈ {1,3,5,7,8,9}, fanout 1–8 | ✅ |
| `routing_*` (4) | A key goes to the child that covers it; range scans start and stop at the right children | 0–3 pivots | ✅ |
| `splice_*` (7) | Child replacement and removal keep `children = pivots + 1`, with pivots sorted | 0–2 pivots, every position | ✅ |
| `hlc_is_strictly_monotonic` | The hybrid clock never repeats or goes back, and orders after anything it observed | any 40-bit time | ✅ |
| `account_tracks_live_bytes`, `victims_respect_threshold_and_budget` | Segment accounting; cleaner victim selection | 1–2 segments | ⏱ times out (`BTreeMap`); gated behind `--cfg kani_slow`. The proptest checks accounting after every checkpoint |

30 harnesses verified.

```sh
cargo install --locked kani-verifier && cargo kani setup
cd crates/sidra && cargo kani
```

## Not covered

- Multiple keys and scans are covered by the model-based proptest
  (`tests/model.rs`), not by these proofs.
- Segment GC and the cleaner's relocation are checked only by the accounting
  invariant in the proptest.
- The model assumes lease expiry is honored. Its safety rests on clock skew
  between processes staying within the lease margin.
