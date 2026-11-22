# betula

A copy-on-write Bε-tree key-value store on object storage (S3, RustFS, anything
`object_store` supports with put-if-absent). Same deployment model as SlateDB:
one writer, many readers, no local disk, no coordinator.

## Design

```
writes ─► WAL objects ─► memtable ─► (frozen) ─► checkpoint: apply batch to tree COW
                                                    │
                                 one segment object + manifest (put-if-absent)
```

- **Bε-tree.** Internal nodes hold pivots, child ids and a message buffer. A
  checkpoint puts the frozen memtable into the root buffer. An overflowing
  buffer flushes the messages for its fullest child into that child,
  recursively. Leaves split by bytes. Empty subtrees are removed. The root
  grows and shrinks as needed.
- **Copy-on-write segments.** A checkpoint writes every node it touches into
  one immutable segment object (`seg/{epoch}-{seq}.seg`) and shares the rest.
  Parents point at children by `(segment, offset, len)`, so reading a node is
  one ranged GET with no lookup. A checkpoint costs about 2 PUTs, however many
  nodes it changes.
- **Manifest.** `manifest/{id}.mf` objects are immutable. The highest id is
  current. Publishing id `n+1` with put-if-absent is the only coordination
  point. A manifest records:
  - the root and its height;
  - the WAL id it covers;
  - the writer epoch;
  - a segment-id watermark;
  - live and total bytes for every referenced segment;
  - the segments that died going from the previous version to this one.
- **WAL.** Batched every `wal_flush_interval` or `wal_max_bytes` into
  `wal/{id}.wal` with put-if-absent. A write is acknowledged, and becomes
  visible, once its WAL object is durable.
- **Fencing.** Opening a writer publishes a manifest with `epoch+1`, then
  writes an empty WAL batch at the next free id. The old writer's next WAL
  write or manifest publish conflicts, and it fails with `Error::Fenced`.
- **Inner-node cache.** Internal and leaf nodes have separate weighted caches.
  The inner cache is warmed on open. A cold point read is one GET (the leaf).
  Concurrent misses on the same node share one fetch.
- **Readers.** `DbReader` serves the latest manifest plus the WAL tail and
  refreshes on an interval.
- **Cleaner.** Segments with less than `clean_below_live` (default 25%) of
  their bytes still live are victims. The next checkpoint rewrites their live
  nodes along with its normal work, which is ordinary copy-on-write. It finds
  them by walking only internal nodes (cached): a leaf is judged by the
  pointer its parent holds. Moves are capped per checkpoint. Cleaning at live
  fraction *t* adds about *t*/(1−*t*) to write amplification and keeps space
  amplification near 1/*t*.
- **GC.** When manifest `m[i]` is older than `retention`, GC deletes the
  segments that died in `m[i]`, then `m[i-1]`, then WALs already folded into
  `m[i]`. On open, the writer also deletes segments at or above the published
  watermark, i.e. uploads a crashed writer never published.

## Guarantees and limits

- Durable after an acknowledged write (default `Durability::AwaitDurable`).
- Readers and snapshots must refresh within the GC retention (default 10 min).
  Otherwise reads fail with `Error::SnapshotExpired`.
- Known gaps:
  - No merging of underfull leaves (only empty ones are removed).
  - Segments uploaded by a fenced zombie writer *after* a newer writer opened
    are not reclaimed.
  - A zombie stalled past the GC retention could write a WAL id that GC
    already deleted.
  - Leaves are fetched whole, with no ranged block reads.

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

### Results (local RustFS, 1M keys × 256 B, defaults: 64 KiB leaves, 1 MiB internal, 16 MiB memtable)

| | betula | SlateDB 0.18 |
|---|---|---|
| load keys/s (p99 ms) | 20.4k (123) | 19.9k (127) |
| PUTs / MiB written during load | 525 / 1441 | 507 / 270 (+ deferred compaction) |
| warm point reads/s | 803k, 0 GETs | 288k, 0 GETs |
| cold point reads (50k): GETs, reads/s | 3.4k, 185k | 37.5k, 17k |
| cold reads/s with 20 ms added per request | 35k | 3.3k |
| 100-key scans/s (20 ms added) | 23k (24k) | 62k (690) |

Load throughput for both engines is at the ceiling set by the 100 ms WAL
interval × 64 clients × batches of 32. betula pays for its read advantage in
bytes written: Bε buffers, plus the cleaner at a 25% threshold.
