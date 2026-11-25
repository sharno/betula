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
mode = "auto"              # or "standby" (backup), or "never" (write-only)
steal_after = "10s"
[gc]
mode = "on"
retention = "10m"
[hedge]
mode = "on"                # a GET slower than the recent p95 gets a second copy
percentile = 0.95
max_extra = 0.1            # at most 10% more GETs
```

```sh
SIDRA_WAL__FLUSH_INTERVAL=20ms SIDRA_APPLIER__MODE=never ./my-service
```

#### Writers in other regions

A writer far from the home bucket can put its log in a bucket in its own
region, so a write costs a local PUT. Manifests, segments and leases stay in
the home bucket, and the applier pulls the regional log from there.

```toml
# A writer in eu-west-1; the database lives in us-east-1.
log = { region = "eu" }
[applier]
mode = "standby"               # apply only while the home region can't
after = "30s"
[regions]
eu = "s3://my-db-eu/orders"
```

A `standby` writer takes the applier role only when work has waited `after`
with no manifest published, and hands it back at its next checkpoint once a
live `auto` writer exists. `mode = "never"` keeps a writer out entirely.

Every process that pulls logs (the applier, readers that follow every
writer, `sidra writers` and `sidra gc`) needs the same `[regions]` table, or
`Db::builder(..).with_region(..)` in code. One without it fails with
`InvalidConfig` when it meets a writer logging there. A write is durable in
its own region first: if that region fails before the applier pulls it, the
write is unavailable until the region recovers, not lost.

#### Readers in other regions

Replicate the home bucket into the reader's region (S3 replication, or any
copy job) and open the replica, naming the original as `origin`:

```toml
# Reader in eu-west-1, opened on s3://my-db-eu-replica/orders.
origin = "s3://my-db/orders"
```

Every object is immutable except that new manifests appear, so a replica is
always a consistent, slightly older database. A segment that hasn't arrived
yet is read from the origin. Limits:
- `Freshness::Latest` is as fresh as the replica, not the origin.
- Replicate deletes too, or expire the replica's objects with a lifecycle
  rule, or the replica keeps every segment GC removed.

Credentials and endpoints come from the usual `AWS_*`, `GOOGLE_*` and
`AZURE_*` variables (for example `AWS_ENDPOINT` and `AWS_ALLOW_HTTP` for
RustFS or MinIO).

### Errors

`err.kind()` says what to do:

| Kind | Meaning |
|---|---|
| `Unavailable` | The store failed after retries. Safe to retry (`err.is_retryable()`) |
| `WriterLost` | Log sealed (presumed dead after its lease lapsed), writer id taken, or a log write's outcome unknown. Open a new writer |
| `NotApplier` | An applier-only call on a process without the role |
| `Closed` | The handle was closed |
| `SnapshotExpired` | A snapshot was held past the GC retention |
| `Corrupt` | Stored data failed validation |
| `InvalidConfig` | Bad settings or URL |

`db.status()` reports a background failure (such as a sealed log) without
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
writer A ─► wal/A/{seq} ─┐
writer B ─► wal/B/{seq} ─┼─► applier: fold into tree COW ─► one segment + manifest (put-if-absent)
writer C ─► wal/C/{seq} ─┘
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
  everything below. A scan positions one cursor in each leaf and in each run
  above it on its path and merges them with a heap one entry at a time.
  Past its first leaf, a scan fetches the next sibling leaves in its range
  ahead (2, then 4, then 8), batched into coalesced ranged GETs per segment;
  a scan that ends in its first leaf fetches nothing extra.
- **Tombstones.** Deletes stay as tombstones until no writer can still
  produce an older version of the key. Each writer's lease
  (`writers/{id}/lease`) carries a floor under every timestamp it may still
  make durable, and the applier drops tombstones below the lowest floor of
  every writer whose log isn't sealed. Three rules make this safe:
  - The applier pulls logs from every writer whose lease it read, after
    reading the leases.
  - A writer stops counting only once its log is **sealed**: a seal record
    put-if-absent at its next sequence number. Log slots are consecutive and
    a writer never skips one, so nothing can land after a seal, and a write
    still in flight either lands first (and is pulled into the same
    checkpoint) or finds the seal and fails unacknowledged. The applier
    seals writers whose lease lapsed; a closing writer seals its own log.
    Lease expiry only says when to seal, so a delayed request or a skewed
    clock can't bring back a deleted key.
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
- Dropping tombstones doesn't depend on clocks. A writer counts until its
  log is sealed, however late its requests arrive; a writer that starts late
  with a lagging clock first moves its clock past a bound below which
  tombstones may already be gone. Clock skew only affects liveness: a
  writer whose lease looks expired to the applier gets sealed and fails with
  `WriterLost`, and a live writer with a stale lease holds tombstones back.
- One writer's unreachable log store (a regional outage) doesn't stall the
  others: their writes keep getting applied, tombstone collection pauses,
  and `Freshness::Latest` reads fail rather than miss writes.
- The unapplied backlog limit counts every unapplied batch held, so
  overwrites of one key apply backpressure too. `close()` skips it to make
  queued writes durable, so it can't hang waiting for an applier.
- By default every writer pulls every other writer's log to show unapplied
  writes, which costs about (writers − 1) × log bytes in GETs. With
  `poll.follow = "manifest_only"` a writer sees others' writes once applied
  (about a checkpoint interval), unless it reads with `Freshness::Latest`.
- GC deletes the leases of sealed, fully applied writers whose lease expired
  over a retention ago, and the applier then drops them from the manifest.
  The seal object stays forever (a few bytes per writer id ever opened),
  so a forgotten writer id can never write again.
- No merging of underfull leaves (only empty ones are removed), so heavy
  deletes leave sparse leaves until rewritten (see the roadmap).
- Store requests retry transient failures with capped backoff (8 attempts,
  50 ms to 5 s). A log write that still fails ends its writer with
  `WriterLost`, since whether it landed is unknown.
- A GET still running after the p95 latency of that store's last 512 GETs
  gets a second copy, and the first answer wins: an object store's slow
  tail is one slow server or connection, which the copy rarely shares. A
  budget caps copies at 10% of GETs, so a store that slows down as a whole
  doesn't get twice the load. PUTs aren't hedged.

## Roadmap

Done: LZ4 compression (on by default), lighter cleaning of inner segments,
paced GC deletes, multi-region (regional writer logs, standby appliers,
replica readers), heap-merged scans, scan readahead, hedged GETs.

1. **Snapshots and clones:** pin a manifest. Nearly free with copy-on-write.
2. **Soak tests:** multi-hour runs against a real store.
3. **Fewer bytes written:** buffers account for most write amplification
   (about 1.8x of message bytes in mixed traffic). Measure and try skipping
   buffer levels for dense batches.
4. **Merging underfull nodes:** after a flush rewrites a leaf below about a
   quarter full, merge it with a sibling (flushing the sibling's runs too) or
   rebalance the pair; the same for internal nodes by child count. Today
   only empty leaves go, so delete-heavy workloads (TTL expiry, queues)
   leave sparse leaves: slower cold scans, a larger cached upper tree, more
   bytes. `sidra-bench --delete-pct 90` measures it.
5. **Optional conditional writes** (compare-and-set) routed through the
   applier, for workloads that need them.

## Verification

See [`spec/README.md`](spec/README.md):
- a TLA+ model of the protocol, model-checked with everystate (every protocol
  rule is shown necessary by a counterexample);
- Kani bounded proofs of the core functions;
- a model-based proptest of the whole system with several concurrent writers
  and late writes;
- the same proptest under injected faults: store requests that fail before
  applying, responses lost after applying, random delays, writers killed
  without closing, and appliers killed part-way through a checkpoint.
  Every acknowledged write must survive, and every read must match the
  model. (`PROPTEST_CASES=400` for stress runs.)

## Layout

- `crates/sidra`: the library.
- `crates/sidra/tests/model.rs`: model-based proptest against a `BTreeMap`
  with tiny nodes and eager cleaning (splits, flushes, removals, relocation,
  reopen, readers). After every checkpoint it checks that the manifest's live
  bytes per segment equal the bytes of reachable nodes. Further tests cover
  fencing, GC (only referenced segments remain, space amplification is
  bounded), and crash orphans.
- `crates/sidra-bench`: sidra vs SlateDB on an S3 endpoint, with a metered
  object store (request counts, bytes, simulated S3 latency).
  `s3probe` measures the store's raw PUT/GET throughput.

## Benchmark

```sh
# RustFS, no docker
rustfs server --address 127.0.0.1:9100 /path/to/data   # RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY
cargo run --release -p sidra-bench -- --keys 1000000 --leaf-kib 256 --internal-kib 2048 --memtable-mib 32
cargo run --release -p sidra-bench -- ... --latency s3     # or a fixed delay: --latency 20ms
```

`--latency s3` adds a delay sampled per request: log-normal time to first
byte (GET p50 20 ms, p99 120 ms; PUT p50 40 ms, p99 250 ms), 1 in 1,000
requests stalling 0.5–3 s, and transfer at 80 MiB/s per request. These are
rough public figures for S3 Standard in-region, not a fit to measurements.
The sequence is seeded, so the n-th request of every run draws the same delay.

Phases: load (random order, batches of 32, 64 clients, durable writes), settle
(checkpoint vs. memtable flush, then wait until SlateDB's manifest stays
unchanged for 10 s, so its compaction is done), warmup (one full scan, so
both caches hold the data), warm random reads, scans (`--scan-len`,
100 keys by default), close and reopen, cold random reads, reopen again,
cold scans, updates, settle. Every read is checked against the
expected value. Both engines get 640 MiB of cache: SlateDB keeps its defaults
(512 MiB blocks + 128 MiB metadata), and sidra uses 256 MiB inner + 384 MiB
leaf.

### Results (local RustFS on one Mac)

The meter counts multipart upload parts. Earlier versions of this table missed
them, which undercounted SlateDB's SST writes; the older tables below carry
that error in their SlateDB byte figures.

Current defaults, 1M keys × 256 B, 40 clients, then 15 minutes of mixed
traffic (80% writes, 20% reads), no added latency. Random (incompressible)
values:

| | sidra | SlateDB 0.18 |
|---|---|---|
| mixed reads/s (p99 ms) | **1.15M (0.01)**, 0.95M–1.27M | 184k (0.69), swings 71k–311k |
| mixed GETs | **59k** | 2.9M |
| mixed writes keys/s | **10.2k** | 9.6k |
| mixed write p99 / p99.9 ms | **150 / 203** | 242 / 304 |
| MiB written, mixed | 9,702 | **6,796** |
| load write p99 ms | 133 | **125** |
| MiB written, load | 884 | **755** |
| cold reads (50k): GETs, reads/s | **1.3k, 398k** | 37.6k, 19k |

JSON-like values, LZ4 on for both engines, 5 minutes mixed:

| | sidra | SlateDB 0.18 |
|---|---|---|
| mixed reads/s | **1.18M** | 207k |
| MiB written, load | **253** | 260 |
| MiB written, mixed | 927 | **774** |
| footprint after run | 1,204 MiB | **1,066 MiB** |

SlateDB's compactor keeps rewriting SSTs under mixed traffic and doesn't
cache its output, so its reads go back to the store.

With compressible data the bytes-written gap narrows to about 1.2× under
mixed traffic. Sidra's GC spreads each run's deletes over its interval: on
RustFS, bursts of deletes pushed write p99 to 0.7–1.2 s.

Reads and scans with S3-like latency: 1M keys × 256 B, 256 KiB leaves,
20 ms added to every request (`--latency 20ms`), 64 clients. Warm phases run
after the warmup, cold ones right after a reopen. SlateDB scans cache their
blocks like sidra caches leaves. No single SlateDB readahead setting wins
every row: small fetches suit warm short scans and large ones suit cold
scans. So each SlateDB scan cell is the best of three settings (read-ahead
KiB / fetch tasks: 4/1, 64/4, 256/8), shown in brackets.

| | sidra | SlateDB 0.18 |
|---|---|---|
| warm point reads/s | **831k** | 372k |
| cold point reads (50k): GETs, reads/s | **1.3k, 81k** | 37.6k, 3.6k |
| warm 100-key scans/s (p99 ms) | **66.6k** (5.0) | 53.2k (2.7) [4/1] |
| cold 100-key scans/s (p99 ms) | **5.0k (49)** | 3.5k (85) [64/4] |
| warm 1,000-key scans/s (p99 ms) | **6.8k (23)** | 5.0k (25) [64/4] |
| cold 1,000-key scans/s (p99 ms) | **5.9k (71)** | 2.4k (99) [64/4] |
| warm 10,000-key scans/s (p99 ms) | **662 (120)** | 550 (143) [256/8] |
| cold 10,000-key scans/s (p99 ms) | **1.4k (166)** | 420 (340) [256/8] |
| warm sparse scans/s, 90% deleted (p99 ms) | **112k (1.3)** | 3.5k (35) [64/4] |
| cold sparse scans/s, 90% deleted (p99 ms) | **5.1k (84)** | 2.4k (136) [64/4] |

The leaves fit in cache, so most cold scans hit leaves earlier scans
loaded; p99 shows the scans that went to the store. Leaf readahead took
sidra's cold 10,000-key scans from 538 to 1.4k/s. SlateDB's cold scans vary
up to 2× between runs; the table keeps its best run.

With simulated S3 latency (`--latency s3`, see above) instead of a fixed
20 ms; same setup, SlateDB scan settings picked the same way. Cold scans show
p50 / p99 / p99.9 ms and scans/s, because one stall can set the length of a
short phase. The middle column is sidra before hedged GETs (`--hedge off`):

| | sidra | sidra, no hedging | SlateDB 0.18 |
|---|---|---|---|
| warm point reads/s | **870k** | 895k | 323k |
| cold point reads/s (p99 / p99.9 ms) | **70k (32 / 86)** | 60k (34 / 101) | 2.6k (122 / 281) |
| warm 100-key scans/s | **65.5k** | 70.5k | 47.5k [4/1] |
| cold 100-key scans | **0.24 / 107 / 139 (3.6k)** | 0.06 / 125 / 1,362 (1.1k) | 13 / 158 / 191 (2.1k) [256/8] |
| warm 1,000-key scans/s | **6.6k** | 6.8k | 5.0k [64/4] |
| cold 1,000-key scans | **0.13 / 130 / 167 (4.0k)** | 0.13 / 147 / 1,434 (1.2k) | 24 / 184 / 238 (2.0k) [256/8] |
| warm 10,000-key scans/s | **662** | 669 | 505 [64/4] |
| cold 10,000-key scans | **1.9 / 273 / 306 (1,055)** | 1.4 / 295 / 1,525 (293) | 138 / 419 / 2,163 (231) [256/8] |
| warm sparse scans/s, 90% deleted | **107k** | 105k | 2.8k [4/1] |
| cold sparse scans | **0.16 / 140 / 189 (3.8k)** | 0.17 / 153 / 278 (3.1k) | 34 / 271 / 590 (1.5k) [256/8] |
| mixed reads/s, 40 clients, 5 min | **1.16M** | 1.18M | 1.7k |
| mixed write p99 / p99.9 ms | **322 / 498** | 366 / 586 | 402 / 1,790 |

Without hedging, a stalled GET held every scan waiting on the leaves in its
batch, and a cold phase makes only about 1,000 GETs, so one stall set the
phase's length. Hedging takes cold-scan p99.9 from 1.4–1.5 s to 139–306 ms
and throughput up 3–4×, for 0–4% more GETs. Warm phases make no GETs, so
their differences are run-to-run noise. SlateDB's mixed reads mostly miss
its cache, so S3 latency takes them from 184k to 1.7k/s.

Commands (SlateDB alone with `--engines slatedb --slatedb-read-ahead-kib N
--slatedb-fetch-tasks N`):

```sh
B="--keys 1000000 --leaf-kib 256 --internal-kib 2048 --memtable-mib 32"
sidra-bench $B --concurrency 40 --mixed-read-pct 20 --mixed-secs 900
sidra-bench $B --concurrency 40 --mixed-read-pct 20 --mixed-secs 300 --values text --compression lz4
sidra-bench $B --latency 20ms --scan-len 100 --scans 2000              # also 1000; 10000 with --scans 500
sidra-bench $B --latency 20ms --scan-len 100 --scans 2000 --delete-pct 90
# --latency s3: the two scan commands above with s3 in place of 20ms (and
# --hedge off for the middle column), and
sidra-bench $B --latency s3 --concurrency 40 --mixed-read-pct 20 --mixed-secs 300
```

Earlier runs:

Single writer, 1M keys × 256 B, defaults (64 KiB leaves, 1 MiB inner-node
budget, 16 MiB per checkpoint). Medians of 3 runs, alternating the two
sidra builds:

| | sidra (in-node buffers) | sidra (append-only runs) | SlateDB 0.18 |
|---|---|---|---|
| load keys/s (p99 ms) | 20.3k (161) | 20.5k (132) | 19.4k (218) |
| MiB written during load | 1,475 | **780** | 270 (+ deferred compaction) |
| warm point reads/s | 931k | 1.03M | 336k |
| open (reopen) | 52 ms | 140 ms (`Background`: 29 ms) | 53 ms |
| cold point reads (50k): GETs, reads/s | 4.0k, 297k | 3.8k, 319k (`Background`: 3.8k, 262k) | 43k, 32k |

At 64 clients × batches of 32, load for both engines sits at the ceiling set
by the 100 ms log flush interval.

SlateDB's numbers in that table predate the compaction wait and the warmup.

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
