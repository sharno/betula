# Formal verification

Two layers:

- **`Sidra.tla`**: a TLA+ model of the single-writer protocol, model-checked
  with [everystate](https://crates.io/crates/everystate), a TLC-compatible
  checker that needs no Java.
- **`crates/sidra/src/proofs.rs`**: [Kani](https://model-checking.github.io/kani/)
  bounded proofs of the core pure functions in the Rust code itself.

## Protocol model (TLA+)

The model covers one key, a WAL of put-if-absent slots, the latest manifest
(`writer_epoch`, tree, `last_ts`, `wal_applied`, `fences`), and writers.

A writer is a small state machine: `idle` -> `reading` -> `placing` -> `open`,
or `dead` (fenced or superseded).
- **Claim**: publish the next manifest with epoch + 1. A conflict retries, so
  the model takes the step whole.
- **Read / Place**: read the WAL forward from `wal_applied + 1`, replaying
  batches. Put `Fence(epoch)` in the first empty slot. A slot taken since the
  read is read on from. A newer fence supersedes the writer. The put comes
  later than the read, so the slot may have changed.
- **Stamp / Land**: stamp one batch (hybrid clock over replayed timestamps and
  `last_ts`, any clock reading), then put it at the next slot. The puts are
  apart, so one can land after a newer fence and after GC. A taken slot means
  fenced: no ack, the writer stops.
- **Plan / Publish**: take a prefix of the unapplied tail, apply it (leaves
  drop tombstones), publish manifest id + 1 put-if-absent with `wal_applied`
  at the end of the prefix, the fences in it, and `last_ts`. A conflict means
  fenced.
- **Gc**: delete WAL slots at or below `wal_applied`, except fences.
- Readers: latest manifest, then the WAL from `wal_applied + 1` to the first
  empty slot, skipping fences.

| Invariant | Meaning |
|---|---|
| `Correct` | A reader sees the last acknowledged write: none lost (also after GC), none resurrected |
| `WriterSees` | A current writer's own view (tree plus tail) has every acknowledged write |
| `NoZombieAck` | No put lands at or after a newer writer's fence |
| `Monotone` | Acknowledged timestamps strictly increase, across writers |

Each switch turns one rule off. The checker finds a counterexample for each,
so the invariants catch these mistakes:

| Config | What it checks | Result |
|---|---|---|
| `Sidra.cfg` | none disabled, 2 writers | pass, 663k distinct states, 4 s |
| `Sidra3.cfg` | none disabled, 3 writers | pass, 6.8M distinct states, 36 s |
| `BugGcFences.cfg` | GC deletes fences: a delayed put lands in a deleted slot | `Correct`, 15 steps |
| `BugUnfenced.cfg` | the new writer places no fence: an old put lands after the replay | `WriterSees`, 7 steps |
| `BugPartial.cfg` | a partial checkpoint sets `wal_applied` past what it applied | `Correct`, 10 steps |
| `BugClock.cfg` | the new writer ignores `last_ts` | `Monotone`, 13 steps |
| `BugOverwrite.cfg` | a checkpoint overwrites the manifest instead of put-if-absent | `NoZombieAck`, 15 steps |
| `GapStalledFence.cfg` | no check after the fence, 3 writers | `Correct`, 23 steps |

`GapStalledFence` turns off `CheckAfterFence`: after its fence put, the
opener looks for manifest id + 1 and stops if it exists. Without it, an
opener reads an empty slot, then stalls past GC retention. Meanwhile an older writer fills the slot,
a third writer replays and checkpoints past it, and GC deletes it. The
opener's fence put then lands in the deleted slot, and its later puts land in
deleted slots: acknowledged, below a newer fence.

Partial checkpoints are safe because a checkpoint takes a prefix in slot
order, and timestamps increase in slot order. Every version left in the WAL is
newer than everything applied, so dropping a tombstone at a leaf can't
resurrect an older one.

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
- One key. The tree is one version; leaf tombstone drops are modelled, the
  buffer levels are not.
- Time is not modelled: GC retention, manifest GC, and a writer stalled past
  retention at a checkpoint publish (its put-if-absent target manifest could
  be deleted).
- One put in flight per writer; no queue of stamped batches.
