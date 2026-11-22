# betula

A copy-on-write Bε-tree key-value store on object storage (S3, RustFS, anything
`object_store` supports with put-if-absent). No local disk and no coordinator,
like SlateDB, but with **any number of concurrent writers**.

## Design

```
writer A ─► writers/A/wal/{seq} ─┐
writer B ─► writers/B/wal/{seq} ─┼─► applier: fold into tree COW ─► one segment + manifest (put-if-absent)
writer C ─► writers/C/wal/{seq} ─┘
```

- **Writers never coordinate.** Each write gets a hybrid logical clock
  timestamp (physical ms, logical counter, writer id) and goes to the
  writer's own log. A write is acknowledged once its log object is durable.
  It is visible right away on that writer, and on others after their next
  poll (p50 ~40 ms, p99 ~90 ms at 100 ms polling).
- **Newest-wins versions.** Wherever two versions of a key meet (memtable,
  buffer, leaf, reads) the larger timestamp wins. Applying writes in any
  order gives the same result, so a slow writer's late entries need no
  special handling.
- **One applier at a time**, held through the manifest itself. Claiming the
  role publishes the next manifest with applier epoch + 1 (put-if-absent),
  which fences the previous applier. It folds every writer's unapplied
  entries into the tree and records each writer's applied sequence number.
  Any writer takes the role over when entries sit unapplied too long. Losing
  the role never affects writing.
- **Bε-tree.** Internal nodes hold pivots, child pointers and a message
  buffer. An overflowing buffer flushes its fullest child's messages down.
  Leaves split by bytes. The root grows and shrinks as needed.
- **Copy-on-write segments.** A checkpoint writes every node it touches into
  one segment object (`seg/{epoch}-{seq}.seg`), which costs about 2 PUTs.
  Parents point at `(segment, offset, len)`, so reading a node is one ranged
  GET.
- **Tombstones.** Deletes stay as tombstones until no writer can still
  produce an older version of the key. Each writer's lease
  (`writers/{id}/lease`) carries a floor under every timestamp it may still
  make durable, and the applier drops tombstones below the lowest live
  floor. Two rules make this safe:
  - The applier pulls logs from every writer whose lease it read, after
    reading the leases.
  - A rewrite carries the parent's buffered versions for that range down
    with it, so a rewritten leaf has no older version of its keys above it.

  Child pointers record each subtree's oldest tombstone, so leaves whose
  tombstones became droppable are found without loading leaves.
- **Inner-node cache.** Internal nodes have their own cache and are loaded on
  open, so a cold point read is one GET.
- **Cleaner.** Segments below 25% live get their live nodes rewritten by the
  next checkpoint. Cleaning at live fraction *t* adds about *t*/(1−*t*) to
  write amplification and keeps space amplification near 1/*t*.
- **GC.** Once a manifest is past retention, GC deletes the segments that died
  in it, the previous manifest, and log entries the manifest already holds.

## Guarantees and limits

- Writes are durable once acknowledged. Values are per-key last-writer-wins by
  hybrid timestamp. A writer sees its own writes right away and other
  writers' within a poll. A batch from one writer is atomic.
- No compare-and-set or transactions across writers (yet).
- Readers and snapshots must refresh within the GC retention (default
  10 min).
- Tombstone safety assumes clock skew between processes stays within the
  lease margin (a quarter of the lease TTL).
- Every writer pulls every other writer's log to show unapplied writes, which
  costs about (writers − 1) × log bytes in GETs.
- No merging of underfull leaves (only empty ones are removed).

## Verification

See [`spec/README.md`](spec/README.md):
- a TLA+ model of the protocol, model-checked with everystate (every protocol
  rule is shown necessary by a counterexample);
- Kani bounded proofs of the core functions;
- a model-based proptest of the whole system with several concurrent writers
  and late writes.

## Layout

- `crates/betula`: the library.
- `crates/betula/tests/model.rs`: model-based proptest against a `BTreeMap`
  with tiny nodes and eager cleaning (splits, flushes, removals, relocation,
  reopen, readers). After every checkpoint it checks that the manifest's live
  bytes per segment equal the bytes of reachable nodes. Further tests cover
  fencing, GC (only referenced segments remain, space amplification is
  bounded), and crash orphans.
- `crates/betula-bench`: betula vs SlateDB on an S3 endpoint, with a metered
  object store (request counts, bytes, optional added latency).
  `s3probe` measures the store's raw PUT/GET throughput.

## Benchmark

```sh
# RustFS, no docker
rustfs server --address 127.0.0.1:9100 /path/to/data   # RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
cargo run --release -p betula-bench -- --keys 1000000 --leaf-kib 256 --internal-kib 2048 --memtable-mib 32
cargo run --release -p betula-bench -- ... --delay-ms 20   # approximate S3 latency
```

Phases: load (random order, batches of 32, 64 clients, durable writes), settle
(checkpoint vs. memtable flush), warm random reads, 100-key scans, close and
reopen, cold random reads, updates, settle. Every read is checked against the
expected value. Both engines get 640 MiB of cache: SlateDB keeps its defaults
(512 MiB blocks + 128 MiB metadata), and betula uses 256 MiB inner + 384 MiB
leaf.

### Results (local RustFS on one Mac)

Single writer, 1M keys × 256 B, defaults (64 KiB leaves, 1 MiB internal, 16 MiB per checkpoint):

| | betula | SlateDB 0.18 |
|---|---|---|
| load keys/s (p99 ms) | 20.4k (147) | 19.9k (124) |
| PUTs / MiB written during load | 558 / 1419 | 506 / 270 (+ deferred compaction) |
| warm point reads/s | 839k, 0 GETs | 357k, 0 GETs |
| cold point reads (50k): GETs, reads/s | 4.4k, 264k | 37.6k, 37k |
| 100-key scans/s | 31k | 82k |

At 64 clients × batches of 32, load for both engines sits at the ceiling set
by the 100 ms log flush interval.

Several writers on one database (500k keys, 64 clients in total, spread
across them). SlateDB allows only one writer:

| writers | load keys/s | update keys/s | cross-writer visibility p50 / p99 |
|---:|---:|---:|---|
| 1 | 20.4k | 20.6k | — |
| 2 | 20.4k | 26.0k | 37 / 89 ms |
| 4 | 20.3k | 16.9k | 47 / 92 ms |
| 8 | 19.7k | 23.0k | 37 / 97 ms |

Capacity, 256 clients × batches of 128, 2M keys:

| | betula, 4 writers | SlateDB, 1 writer |
|---|---|---|
| load keys/s (p99 ms) | 203k (399) | 307k (162) |
| update keys/s (p99 ms) | 305k (174) | 294k (119) |

Bulk loading into an empty tree is limited by the single applier's
copy-on-write rewrites. betula pays for its read advantage in bytes written:
Bε buffers, plus the cleaner.
