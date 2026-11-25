# Sans-IO protocol core

The protocol (opening, the log, checkpoints, readers, GC) is plain Rust state
machines with no I/O, clocks, or tasks, in `sidra::protocol`. The async
drivers and a Stateright model both drive the same machines, so the model
checks the code that runs.

## Split

| Layer | Contents | I/O |
|---|---|---|
| `protocol` | What to request next, what a response means, when to ack, fence, checkpoint, delete | none |
| `tree`, `nodes`, `segments`, `codec` | Data plane: Bε-tree rewrite, node cache, segment packing | node loads only, via the driver |
| `db`, `reader`, `follow` | Drivers: run effects on `object_store`, timers, channels, metrics | all |
| `crates/sidra-model` | A Stateright model driving the same machines against an abstract store | simulated |

Time is an input (`now`), never read inside. Independent clocks in the model
are different `now` values per process.

## Machines

- **`fence`** (typed transitions): opening a writer after it claimed the next
  epoch. `Reading` reads the log forward from the applied slot; at the first
  empty slot it becomes `Placing`, which puts the fence. A taken slot reads
  on; a newer writer's fence is `Superseded`. The driver then checks for a
  newer manifest.
- **`log`** (hot path): batching, slot numbers, put-if-absent,
  acknowledgement, fenced or unknown outcomes, backpressure input.
- **`tail`**: unapplied batches and fences past the manifest, as the writer
  and readers hold them. `Scan` reads the log forward to the first empty
  slot; `Tail::take` picks a checkpoint's prefix.
- **`checkpoint`**: whether one is due, what it applies (`plan`), and the
  manifest it publishes (`next_manifest`).
- **`gc`**: which manifests, segments and log slots may go.

## Stateright model

- Processes each open a writer (fencing the previous one), write a script,
  and checkpoint; one actor is the store (put-if-absent, lost requests and
  responses); processes crash; GC deletes applied log slots.
- The data plane is abstract: a checkpoint's tree is one key→version map that,
  like a leaf, keeps no tombstones.
- A store request and its response run as one step (a sound partial-order
  reduction: handling a response touches only the requester's state).
- Properties: acknowledged and durable writes stay visible to a fresh reader;
  log order is timestamp order; a tombstone gets dropped and a writer gets
  fenced (non-vacuity).
- Exhaustive search of the two-writer scenario: 3.4M states in 11 s, 82.6M
  with one fault in 6 min (`--ignored`). Turning off a rule (the fence, kept
  fences, `last_ts`) fails a property within a second; `tests/trace.rs`
  replays each scenario.
- It found that two openers in the same millisecond could claim
  byte-identical manifests and each take the other's as its own retried
  claim. A claim now counts only a fresh put as its own.
