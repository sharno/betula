# Formal verification

Two layers:

- **`Sidra.tla`**: a TLA+ model of the single-writer protocol, model-checked
  with [everystate](https://crates.io/crates/everystate), a TLC-compatible
  checker that needs no Java.
- **`crates/sidra/src/proofs.rs`**: [Kani](https://model-checking.github.io/kani/)
  bounded proofs of the core pure functions in the Rust code itself.

## Protocol model (TLA+)

The model covers one key, a WAL of put-if-absent slots, manifests by id
(`writer_epoch`, `writer`, `parent`, tree, `last_ts`, `wal_applied`, `fences`),
and writers. The latest manifest is the highest id present.

A writer is a small state machine: `idle` -> `reading` -> `placing` -> `open`,
or `dead` (fenced or superseded).
- **ClaimRead / ClaimPut**: read the latest manifest, then put the next one
  (epoch + 1, a fresh `writer` id, `parent` = the copied manifest's `writer`)
  put-if-absent. A conflict
  retries. The put comes later than the read: if GC collected the id, it
  succeeds below the latest, a stray. The opener then lists; any id above its
  own (a newer opener's claim, or manifests past a stray) stops it. Retrying
  instead lets two openers supersede each other forever.
- **Read / Place**: read the WAL forward from `wal_applied + 1`, replaying
  batches. Put `Fence(epoch)` in the first empty slot. A slot taken since the
  read is read on from. A newer fence supersedes the writer. The put comes
  later than the read, so the slot may have changed. After the fence, any
  manifest id above the claimed one means stop.
- **Stamp / Land**: stamp one batch (hybrid clock over replayed timestamps and
  `last_ts`, any clock reading), then put it at the next slot. The puts are
  apart, so one can land after a newer fence and after GC. A taken slot means
  fenced: no ack, the writer stops.
- **Plan / Publish**: take a prefix of the unapplied tail, apply it (leaves
  drop tombstones), put manifest id + 1 put-if-absent with `wal_applied`
  at the end of the prefix, the fences in it, `last_ts`, and `writer` = `parent`
  = its own id. A conflict means fenced. If GC collected that id since the
  plan, the put succeeds (a stray). The writer lists after the put; any id
  above its own means it stops and does not install the manifest.
- **Gc**: list manifests in ascending order, take the lowest pair (prev, cur).
  The newest is never deleted. If `cur.id = prev.id + 1` and
  `cur.parent = prev.writer`, delete prev and the WAL slots at or below
  `cur.wal_applied`, except fences. Otherwise prev is a stray: only prev goes.
  The model treats everything as past retention.
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
| `Sidra.cfg` | none disabled, 2 writers | pass, 6.6M distinct states, 51 s |
| `Sidra3.cfg` | none disabled, 3 writers | pass, 30.2M distinct states, 241 s |
| `BugGcFences.cfg` | GC deletes fences: a delayed put lands in a deleted slot | `Correct`, 18 steps |
| `BugUnfenced.cfg` | the new writer places no fence: an old put lands after the replay | `WriterSees`, 8 steps |
| `BugPartial.cfg` | a partial checkpoint sets `wal_applied` past what it applied | `Correct`, 10 steps |
| `BugClock.cfg` | the new writer ignores `last_ts` | `Monotone`, 14 steps |
| `BugOverwrite.cfg` | a checkpoint overwrites the manifest instead of put-if-absent | `WriterSees`, 16 steps |
| `BugParentEpoch.cfg` | a manifest's parent is a writer epoch, not a unique id, 3 writers | `Correct`, 25 steps |
| `BugConfirmPublish.cfg` | no list after a claim or checkpoint put, 2 writers | `NoZombieAck`, 22 steps |
| `GapStalledFence.cfg` | no check after the fence, 3 writers | `Correct`, 28 steps |

`GapStalledFence` turns off `CheckAfterFence`: after its fence put, the
opener looks for manifest id + 1 and stops if it exists. Without it, an
opener reads an empty slot, then stalls past GC retention. Meanwhile an older writer fills the slot,
a third writer replays and checkpoints past it, and GC deletes it. The
opener's fence put then lands in the deleted slot, and its later puts land in
deleted slots: acknowledged, below a newer fence.

`BugParentEpoch` sets `ParentId = "epoch"`. Epochs are not unique: two openers
that read the same latest manifest both take epoch + 1. One wins id 1, the
other stalls. GC collects the low ids. The stalled claim and a stalled
checkpoint by the first writer land in the collected ids 1 and 2: consecutive,
and the second names epoch 1 as its parent. GC takes the two strays for a
chain and deletes WAL slots up to the stray checkpoint's `wal_applied`, past the
latest manifest's. An acknowledged write is lost. With `ParentId = "writer"`
each open draws a fresh id (bounded by `MaxOpens`: 5 for two writers, 3 for three, to keep the state space small), so a stray's
successor never names it.

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
- Time is not modelled: GC retention is "everything is past it". A stall is
  any gap between a read and its put. The list after a manifest put is atomic
  with the put.
- One put in flight per writer; no queue of stamped batches.
