# sidra

*Sidra* (سدرة) is the lote tree: deep-rooted, drought-hardy and long-lived,
the tree of Sidrat al-Muntaha. A fitting name for a tree-shaped store meant
to outlast the machines that use it.

A copy-on-write Bε-tree key-value store on object storage (S3, RustFS, anything
`object_store` supports with put-if-absent). No local disk and no coordinator,
like SlateDB, but with **any number of concurrent writers**.

## Quickstart

```rust
use sidra::{Db, Freshness, ReadOptions, WriteBatch};

let db = Db::open("s3://bucket/mydb").await?;     // also gs://, az://, file://, memory://
db.put("user:1", "ada").await?;                    // durable when it returns

let mut batch = WriteBatch::new();                 // atomic, one timestamp
batch.put("user:2", "grace").delete("user:3");
db.write(batch).await?;

let v = db.get("user:1").await?;
let fresh = db.get_with("user:1", ReadOptions { freshness: Freshness::Latest }).await?;
let users = db.scan("user:".."user;").collect().await?;  // or .into_stream()
db.close().await?;
```

Open the same URL from any number of processes to get any number of
writers. `DbReader::open(url)` gives a read-only handle. Examples:
`cargo run --example quickstart -- <url>` and `cargo run --example multi_writer`.

For more control, use the builder:

```rust
let db = Db::builder("mydb", store)              // any Arc<dyn ObjectStore>
    .with_settings(Settings::from_file("sidra.toml")?)
    .build()
    .await?;
```

### Configuration

Every setting has a default. `Settings::load()` (used by `Db::open`) layers:
1. the defaults;
2. the TOML file named by `SIDRA_CONFIG`;
3. `SIDRA_*` environment variables, with `__` between sections.

```toml
warm = "blocking"          # or "background": open ~4x faster, reads warm up after
[wal]
flush_interval = "50ms"    # write latency vs PUTs per second
[checkpoint]
bytes = 33554432
interval = "2s"
[poll]
interval = "200ms"         # how fast writers see each other
max_interval = "2s"        # back-off while idle
[applier]
mode = "auto"              # or "never" for write-only processes
steal_after = "10s"
[gc]
mode = "on"
retention = "10m"
```

```sh
SIDRA_WAL__FLUSH_INTERVAL=20ms SIDRA_APPLIER__MODE=never ./my-service
```

Credentials and endpoints come from the usual `AWS_*`, `GOOGLE_*` and
`AZURE_*` variables (for example `AWS_ENDPOINT` and `AWS_ALLOW_HTTP` for
RustFS or MinIO).

### Errors

`err.kind()` says what to do:

| Kind | Meaning |
|---|---|
| `Unavailable` | The store failed after retries. Safe to retry (`err.is_retryable()`) |
| `WriterLost` | Lease expired or writer id taken. Open a new writer |
| `NotApplier` | An applier-only call on a process without the role |
| `Closed` | The handle was closed |
| `SnapshotExpired` | A snapshot was held past the GC retention |
| `Corrupt` | Stored data failed validation |
| `InvalidConfig` | Bad settings or URL |

`db.status()` reports a background failure (such as a lost lease) without
having to write. Call `db.close().await` before exit: dropping a writer warns
and may lose writes that were only queued.

### Metrics

`db.metrics()` returns:
- the applier role and epoch, plus manifest id and age;
- unapplied bytes and how long the oldest has waited;
- segment and live bytes;
- batches and log flushes written;
- checkpoints run and the last one's duration;
- node cache occupancy, hits and misses;
- object store GETs, PUTs, LISTs and DELETEs, with byte counts.

### Command line

```sh
cargo install --path crates/sidra-cli      # installs `sidra`
sidra info    s3://bucket/db               # manifest, applier, tree, space
sidra writers s3://bucket/db               # leases and unapplied logs
sidra get     s3://bucket/db user:1        # latest acknowledged value
sidra scan    s3://bucket/db --prefix user: --limit 20
sidra put     s3://bucket/db user:1 ada    # takes the applier role only if none is running
sidra delete  s3://bucket/db user:1
sidra shape   s3://bucket/db               # walks the whole tree
sidra gc      s3://bucket/db --retention 10m
```

The same operations are available as a library: `sidra::Admin`.

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
  role publishes the next manifest with applier epoch + 1 and the claimant's
  id (put-if-absent), which fences the previous applier. A closing applier
  releases the role. Another writer takes it over when there's work and the
  role is free, the applier's lease has expired, or nothing was published
  for `steal_after`. It folds every writer's unapplied
  entries into the tree and records each writer's applied sequence number.
  Any writer takes the role over when entries sit unapplied too long. Losing
  the role never affects writing.
- **Bε-tree with append-only buffers.** Internal nodes hold pivots and, for
  each child, a pointer plus a list of buffered message runs. A batch reaching
  a node is split by child and appended as new runs, so only the small
  routing node is rewritten, not the buffered data.
  - When a node's runs exceed its byte budget, or a child has too many runs,
    the fullest child is flushed: its runs are merged into it.
  - Leaves split by bytes. The root grows and shrinks as needed.
- **Copy-on-write segments.** A checkpoint writes the nodes it touches into
  two segment objects (`seg/{epoch}-{seq}.seg`), about 3 PUTs in all: one
  for inner nodes and runs, one for leaves. Warming inner nodes is then a few
  contiguous reads, and short-lived runs don't keep leaf segments alive.
  Parents point at `(segment, offset, len)`, so reading a node is one ranged
  GET.
- **Reads stop early.** Each child pointer records the newest timestamp in its
  subtree. A point read stops once the version it holds is newer than
  everything below. Scans build each leaf's view one page at a time.
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
- **Inner-node cache.** Internal nodes and runs have their own cache. By
  default, open loads both before returning (`Warm::Blocking`), so reads are
  full speed from the first request. `Warm::Background` returns after the
  internal nodes and streams runs in behind, opening about 4x faster. Once warm, a cold
  point read is at most one GET. Polling for other writers backs off while
  nothing changes.
- **Cleaner.** Segments below 25% live get their live nodes rewritten by the
  next checkpoint. Runs in such a segment are copied in place instead of
  being pushed down. Cleaning at live fraction *t* adds about *t*/(1−*t*) to
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
- By default every writer pulls every other writer's log to show unapplied
  writes, which costs about (writers − 1) × log bytes in GETs. With
  `poll.follow = "manifest_only"` a writer sees others' writes once applied
  (about a checkpoint interval), unless it reads with `Freshness::Latest`.
- GC deletes the leases of writers that expired long ago and have no logs
  left, and the applier then drops them from the manifest.
- No merging of underfull leaves (only empty ones are removed).

## Roadmap

Done: LZ4 compression (on by default), lighter cleaning of inner segments,
paced GC deletes.

1. **Multi-region writers.**
   - Each writer appends its log to a bucket in its own region (local write
     latency, no cross-region traffic to write); its lease records where.
   - The applier is pinned to the home region and pulls those logs
     asynchronously.
   - Regional read replicas follow a replicated manifest. Segments are
     immutable, so bucket replication copies them safely.
2. **Snapshots and clones:** pin a manifest. Nearly free with copy-on-write.
3. **Fault-injection and soak tests:** failed, slow and lost store requests;
   long runs.
4. **Faster scans and bulk loads:** fewer sources per scan page; bottom-up
   build into an empty tree.
5. **Optional conditional writes** (compare-and-set) routed through the
   applier, for workloads that need them.

## Verification

See [`spec/README.md`](spec/README.md):
- a TLA+ model of the protocol, model-checked with everystate (every protocol
  rule is shown necessary by a counterexample);
- Kani bounded proofs of the core functions;
- a model-based proptest of the whole system with several concurrent writers
  and late writes.

## Layout

- `crates/sidra`: the library.
- `crates/sidra/tests/model.rs`: model-based proptest against a `BTreeMap`
  with tiny nodes and eager cleaning (splits, flushes, removals, relocation,
  reopen, readers). After every checkpoint it checks that the manifest's live
  bytes per segment equal the bytes of reachable nodes. Further tests cover
  fencing, GC (only referenced segments remain, space amplification is
  bounded), and crash orphans.
- `crates/sidra-bench`: sidra vs SlateDB on an S3 endpoint, with a metered
  object store (request counts, bytes, optional added latency).
  `s3probe` measures the store's raw PUT/GET throughput.

## Benchmark

```sh
# RustFS, no docker
rustfs server --address 127.0.0.1:9100 /path/to/data   # RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
cargo run --release -p sidra-bench -- --keys 1000000 --leaf-kib 256 --internal-kib 2048 --memtable-mib 32
cargo run --release -p sidra-bench -- ... --delay-ms 20   # approximate S3 latency
```

Phases: load (random order, batches of 32, 64 clients, durable writes), settle
(checkpoint vs. memtable flush), warm random reads, 100-key scans, close and
reopen, cold random reads, updates, settle. Every read is checked against the
expected value. Both engines get 640 MiB of cache: SlateDB keeps its defaults
(512 MiB blocks + 128 MiB metadata), and sidra uses 256 MiB inner + 384 MiB
leaf.

### Results (local RustFS on one Mac)

The meter counts multipart upload parts. Earlier versions of this table missed
them, which undercounted SlateDB's SST writes; the older tables below carry
that error in their SlateDB byte figures.

Current defaults, 1M keys × 256 B, 40 clients, then 15 minutes of mixed
traffic (80% writes, 20% reads). Random (incompressible) values:

| | sidra | SlateDB 0.18 |
|---|---|---|
| mixed reads/s (p99 ms) | **1.61M (0.01)**, flat | 311k (0.40), swings 168k–437k |
| mixed GETs | **177k** | 4.6M |
| mixed writes keys/s | **10.2k** | 9.4k |
| mixed write p99 / p99.9 ms | **145 / 181** | 256 / 1,183 |
| MiB written, mixed | 9,123 | **6,523** |
| load write p99 ms | **131** | 144 |
| MiB written, load | 830 | **755** |
| cold reads (20k): GETs, reads/s | **3.6k, 158k** | 18.4k, 27k |

JSON-like values, LZ4 on for both engines, 5 minutes mixed:

| | sidra | SlateDB 0.18 |
|---|---|---|
| mixed reads/s | **1.49M** | 324k |
| MiB written, load | 248 | **171** |
| MiB written, mixed | 932 | **864** |
| footprint after run | 1,195 MiB | **1,066 MiB** |

With compressible data the bytes-written gap narrows to about 1.1× under
mixed traffic. Sidra's GC spreads each run's deletes over its interval: on
RustFS, bursts of deletes pushed write p99 to 0.7–1.2 s.

Earlier runs:

Single writer, 1M keys × 256 B, defaults (64 KiB leaves, 1 MiB inner-node
budget, 16 MiB per checkpoint). Medians of 3 runs, alternating the two
sidra builds:

| | sidra (in-node buffers) | sidra (append-only runs) | SlateDB 0.18 |
|---|---|---|---|
| load keys/s (p99 ms) | 20.3k (161) | 20.5k (132) | 19.4k (218) |
| MiB written during load | 1,475 | **780** | 270 (+ deferred compaction) |
| warm point reads/s | 931k | 1.03M | 336k |
| 100-key scans/s | 33k | 36k | 80k |
| open (reopen) | 52 ms | 140 ms (`Background`: 29 ms) | 53 ms |
| cold point reads (50k): GETs, reads/s | 4.0k, 297k | 3.8k, 319k (`Background`: 3.8k, 262k) | 43k, 32k |

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

| | sidra, 4 writers | SlateDB, 1 writer |
|---|---|---|
| load keys/s (p99 ms) | 203k (399) | 307k (162) |
| update keys/s (p99 ms) | 305k (174) | 294k (119) |

Bulk loading into an empty tree is limited by the single applier's
copy-on-write rewrites. sidra pays for its read advantage in bytes written:
Bε buffers, plus the cleaner.
