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
  on; a newer writer's fence is `Superseded`. The driver then checks that
  no newer manifest is listed.
- **`log`** (hot path): batching, slot numbers, put-if-absent,
  acknowledgement, fenced or unknown outcomes, backpressure input.
- **`tail`**: unapplied batches and fences past the manifest, as the writer
  and readers hold them. `Scan` reads the log forward to the first empty
  slot; `Tail::take` picks a checkpoint's prefix.
- **`checkpoint`**: whether one is due, what it applies (`plan`), and the
  manifest it publishes (`next_manifest`).
- **`gc`**: which manifests, segments and log slots may go, and which
  manifests are strays: put by a writer stalled past the retention into an
  id GC collected.

## Stateright model

- Processes each open a writer (fencing the previous one), write a script,
  and checkpoint; one actor is the store (put-if-absent, lost requests and
  responses); processes crash; GC deletes old manifests, their dead
  segments and applied log slots, as if all were past the retention. A
  request in flight across a GC is a process stalled past the retention.
- The data plane is abstract: a checkpoint's tree is one key→version map that,
  like a leaf, keeps no tombstones.
- A store request and its response run as one step (a sound partial-order
  reduction: handling a response touches only the requester's state).
- Properties: acknowledged and durable writes stay visible to a fresh reader;
  log order is timestamp order; a tombstone gets dropped, a writer gets
  fenced, and a stray manifest lands (non-vacuity).
- The model's tree is one segment per checkpoint, so it can't show what a
  stray does to shared segments or orphans; the library tests do.
- Exhaustive search of the two-writer scenario: 8.1M states in 15 s, 158M
  with one fault in 46 min (`--ignored`). Turning off a rule (the fence, kept
  fences, `last_ts`) fails a property within a second; `tests/trace.rs`
  replays each scenario.
- It found that two openers in the same millisecond could claim
  byte-identical manifests and each take the other's as its own retried
  claim. A claim now names its opening's random writer id.
- It found that an opener retrying when a newer manifest is listed after
  its claim can be superseded forever by another doing the same (the search
  never ended). Such an opener now stops.
