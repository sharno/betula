# betula

A copy-on-write Bε-tree key-value store on object storage (S3, RustFS, anything
`object_store` supports with put-if-absent). Same deployment model as SlateDB:
one writer, many readers, no local disk, no coordinator.

## Design

```
writes ─► WAL objects ─► memtable ─► (frozen) ─► checkpoint: apply batch to tree COW
                                                    │
                                    new nodes + manifest (put-if-absent)
```

- **Bε-tree.** Internal nodes hold pivots, child ids and a message buffer. A
  checkpoint puts the frozen memtable into the root buffer. An overflowing
  buffer flushes the messages for its fullest child into that child,
  recursively. Leaves split by bytes. Empty subtrees are removed. The root
  grows and shrinks as needed.
- **Copy-on-write.** Nodes are immutable objects (`node/{epoch}-{seq}.node`).
  A checkpoint writes new nodes for every node it touches and shares the rest.
- **Manifest.** `manifest/{id}.mf` objects are immutable. The highest id is
  current. Publishing id `n+1` with put-if-absent is the only coordination
  point. A manifest records the root, its height, the WAL id it covers, the
  writer epoch, a node-id watermark, and the nodes it freed from the previous
  version.
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
- **GC.** When manifest `m[i]` is older than `retention`, GC deletes the nodes
  `m[i]` freed, then `m[i-1]`, then WALs already folded into `m[i]`. On open,
  the writer also deletes nodes at or above the published watermark, i.e.
  uploads a crashed writer never published.

## Guarantees and limits

- Durable after an acknowledged write (default `Durability::AwaitDurable`).
- Readers and snapshots must refresh within the GC retention (default 10 min).
  Otherwise reads fail with `Error::SnapshotExpired`.
- Known gaps:
  - No merging of underfull leaves (only empty ones are removed).
  - Nodes uploaded by a fenced zombie writer *after* a newer writer opened are
    not reclaimed.
  - A zombie stalled past the GC retention could write a WAL id that GC
    already deleted.
  - Leaves are fetched whole, with no ranged block reads.

## Layout

- `crates/betula`: the library.
- `crates/betula/tests/model.rs`: model-based proptest against a `BTreeMap`
  with tiny nodes (splits, flushes, removals, reopen, readers), plus tests for
  fencing, GC (only the live tree remains), and crash orphans.
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
