# sidra

*Sidra* (سدرة) is the lote tree: deep-rooted, drought-hardy and long-lived,
the tree of Sidrat al-Muntaha. A fitting name for a tree-shaped store meant
to outlast the machines that use it.

A copy-on-write Bε-tree key-value store on object storage (S3, RustFS, anything
`object_store` supports with put-if-absent). No local disk and no coordinator,
like SlateDB: one writer, any number of readers.

## Quickstart

```rust
use sidra::{Db, DbReader, Freshness, ReadOptions, WriteBatch};

let db = Db::open("s3://bucket/mydb").await?;     // also gs://, az://, file://, memory://
db.put("user:1", "ada").await?;                    // durable when it returns

let mut batch = WriteBatch::new();                 // atomic, one timestamp
batch.put("user:2", "grace").delete("user:3");
db.write(batch).await?;

let v = db.get("user:1").await?;
let users = db.scan("user:".."user;").collect().await?;  // or .into_stream()
db.close().await?;

let reader = DbReader::open("s3://bucket/mydb").await?;  // any number, anywhere
let fresh = reader.get_with("user:1", ReadOptions { freshness: Freshness::Latest }).await?;
```

Opening a writer fences the one already open, if any: its next write or
checkpoint fails with `WriterLost`. So a restarted or replacement process
takes over without coordination, and a stalled one can't corrupt anything.
Examples:
`cargo run --example quickstart -- <url>` and `cargo run --example follow`.

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
interval = "2s"            # also how often an idle writer checks it wasn't fenced
max_bytes = 67108864       # per checkpoint
max_unapplied_bytes = 536870912  # writes stall past this backlog
[gc]
mode = "on"
retention = "10m"
[hedge]
mode = "on"                # a GET slower than the recent p95 gets a second copy
percentile = 0.95
max_extra = 0.1            # at most 10% more GETs
initial = "250ms"          # threshold until 64 GETs are measured
```

```sh
SIDRA_WAL__FLUSH_INTERVAL=20ms ./my-service
```

Readers (`DbReader`) refresh every `refresh_interval` (500 ms): a new
manifest if there is one, then the log past it.

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
| `WriterLost` | Fenced by a newer writer, or a log write's outcome unknown. Open a new writer if this process should still write |
| `Closed` | The handle was closed |
| `SnapshotExpired` | A snapshot was held past the GC retention |
| `Corrupt` | Stored data failed validation |
| `InvalidConfig` | Bad settings or URL |

`db.status()` reports a background failure (such as being fenced) without
having to write. Call `db.close().await` before exit: dropping a writer warns
and may lose writes that were only queued.

### Metrics

`db.metrics()` returns:
- the writer's epoch, plus manifest id and age;
- unapplied bytes and how long the oldest has waited;
- segment and live bytes;
- batches and log flushes written;
- checkpoints run and the last one's duration;
- node cache occupancy, hits and misses;
- object store GETs, PUTs, LISTs and DELETEs, with byte counts.

### Command line

```sh
cargo install --path crates/sidra-cli      # installs `sidra`
sidra info    s3://bucket/db               # manifest, writer epoch, tree, space
sidra log     s3://bucket/db               # log objects, unapplied, fences
sidra get     s3://bucket/db user:1        # latest acknowledged value
sidra scan    s3://bucket/db --prefix user: --limit 20
sidra put     s3://bucket/db user:1 ada --take-over   # fences the running writer
sidra delete  s3://bucket/db user:1 --take-over
sidra shape   s3://bucket/db               # walks the whole tree
sidra gc      s3://bucket/db --retention 10m
```

The same operations are available as a library: `sidra::Admin`.

## Design

```
writer ─► wal/{seq} ─► checkpoint: fold a prefix into tree COW ─► segments + manifest (put-if-absent)
                 └──► readers: manifest + log past it
```

- **One writer, fenced on open.** Opening claims the next manifest with
  writer epoch + 1 (put-if-absent; the last opener wins). It then reads the
  log forward from the manifest's applied slot and puts a fence record,
  put-if-absent, in the first empty slot. Every batch an older writer made
  durable is before the fence, and gets replayed; its next put lands on the
  fence and fails unacknowledged. Its next checkpoint finds the manifest id
  taken and stops it too. Last, the opener checks that no newer manifest is
  listed: one stalled long enough could otherwise fence a slot GC already
  cleared.
- **Stalls past the GC retention.** A claim or checkpoint can put its
  manifest into an id GC already collected. So after each put the writer
  lists manifests, and any newer one means it is out: it stops as fenced
  (`WriterLost`), an opener too. Claiming again instead would let two
  openers supersede each other forever. Each opening draws a random writer id,
  and each manifest names the one that published its parent; GC deletes
  manifests in id order. A stray then sits below every published manifest,
  and the one above names another parent, so GC deletes the stray without
  acting on its dead segments. Epochs can't tell them apart: two openers
  on the same manifest take the same epoch.
- **One log.** Each write gets a hybrid clock timestamp (physical ms, logical
  counter) and goes to the next log slot (`wal/{seq}`). A write is
  acknowledged once its slot is durable, and readable on the writer right
  away. Timestamps are stamped in log order, and a new writer starts its
  clock past everything it replayed and the manifest's newest applied
  timestamp (`last_ts`).
- **Newest-wins versions.** Wherever two versions of a key meet (memtable,
  buffer, leaf, reads) the larger timestamp wins.
- **Checkpoints apply a prefix.** A checkpoint folds the oldest unapplied
  batches, at most `checkpoint.max_bytes` (64 MiB), into the tree and
  publishes the next manifest (put-if-absent) with the last slot applied and
  the fences in that range. Timestamps grow along the log, so everything
  left is newer than everything applied.
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
- **Tombstones.** A delete reaching a leaf goes: no older version of the
  key can arrive later. Older versions are in the tree or in the applied
  prefix, and a rewrite carries the parent's buffered versions for that
  range down with it, so a rewritten leaf has no older version of its keys
  above it.

  A tombstone usually reaches its leaf when a flush rewrites it anyway.
  Once buffered for `purge_tombstones_after` (60 s), a checkpoint
  rewrites its subtree just to drop it, even with no new writes. A tombstone
  still buffered in a run waits until its buffer has had no writes that
  long, then is carried down: a busy buffer flushes on its own, and forcing
  it early would cost its batching. Child pointers summarize when each
  subtree is due, so due ones are found without loading anything. A
  checkpoint starts no new purge, at any depth, once its purges loaded and
  wrote `purge_bytes_per_checkpoint` (64 MiB), so a mass delete spreads
  over several checkpoints instead of stalling the writer on one. Loads
  count: purging a leaf whose keys are all deleted writes nothing.
- **Inner-node cache.** Internal nodes and runs have their own cache. By
  default, open loads both before returning (`Warm::Blocking`), so reads are
  full speed from the first request. `Warm::Background` returns after the
  internal nodes and streams runs in behind, opening about 4x faster. Once warm, a cold
  point read is at most one GET.
- **Cleaner.** Segments below 25% live get their live nodes rewritten by the
  next checkpoint. Runs in such a segment are copied in place instead of
  being pushed down. Cleaning at live fraction *t* adds about *t*/(1−*t*) to
  write amplification and keeps space amplification near 1/*t*.
- **GC.** Once a manifest is past retention, GC deletes the segments that died
  in it, the previous manifest, and log slots the manifest already applied,
  except fences. A fence stays so that a fenced writer's put, however
  delayed, still finds it. Manifests go one at a time, oldest first.

## Guarantees and limits

- Writes are durable once acknowledged. Values are per-key last-writer-wins by
  hybrid timestamp. The writer sees its own writes right away, readers
  within a refresh. A batch is atomic.
- One writer at a time. Writes from other processes go through it (an RPC
  of your own), or each opens the writer in turn, fencing the last.
- Readers and snapshots must refresh within the GC retention (default
  10 min).
- Correctness doesn't depend on clocks. A lagging clock on a new writer
  is moved past `last_ts` and the replayed log.
- Writes stall once unapplied batches reach `max_unapplied_bytes`. Under
  sustained overload, writes go at the checkpoints' pace. Each checkpoint
  frees up to `checkpoint.max_bytes` at once, so a write stalls for up to
  about one checkpoint. The limit counts every unapplied batch held, so
  overwrites of one key apply backpressure too. `close()` skips it to make
  queued writes durable.
- Fences stay forever: a few bytes per writer ever opened.
- A writer stalled past the GC retention stops when it next publishes, and
  an opener stalled past it fails with `WriterLost`. The segments its stray checkpoint uploaded leak.
- No merging of underfull leaves (only empty ones are removed), so heavy
  deletes leave sparse leaves until rewritten (see the roadmap).
- Store requests retry transient failures with capped backoff (8 attempts,
  50 ms to 5 s). A log write that still fails ends the writer with
  `WriterLost`, since whether it landed is unknown.
- A GET still running after the p95 latency of that store's last 512 GETs
  gets a second copy, and the first answer wins: an object store's slow
  tail is one slow server or connection, which the copy rarely shares. A
  process that just opened uses `hedge.initial` (250 ms) until it has 64
  GETs: its cache is cold, so a stall in its first GETs hurts most. A
  budget caps copies at 10% of GETs, so a store that slows down as a whole
  doesn't get twice the load. PUTs aren't hedged.

## Roadmap

Done: LZ4 compression (on by default), lighter cleaning of inner segments,
paced GC deletes, replica readers, heap-merged scans, scan readahead, hedged
GETs, purging buffered tombstones, one fenced writer (replacing concurrent
writers with leases and an applier role: tag `multi-writer`).

1. **Snapshots and clones:** pin a manifest. Nearly free with copy-on-write.
2. **Soak tests:** multi-hour runs against a real store.
3. **Fewer bytes written:** buffers account for most write amplification
   (about 1.8x of message bytes in mixed traffic). Measure and try skipping
   buffer levels for dense batches.
4. **Merging underfull leaves:** after deleting 90% of keys spread evenly
   and purging, leaves average 9% full: 1,310 leaves where 120 would do.
   96% of the underfull leaves a checkpoint writes have a neighbour it also
   rewrote that both fit in, so merging only those needs no extra reads.
   Cold scans of the survivors already cost about the dense tree's GETs, so
   the gain is fewer leaves (cache, GETs per key), not faster scans.
   Contiguous deletes (TTL, queues) need no merging: their leaves empty and
   go (`--delete-order prefix --delete-pct 90`: 1,308 leaves to 179).
5. **Conditional writes** (compare-and-set): the single writer makes them
   cheap.
6. **sidra-server:** the writer behind an RPC, for many client processes.

## Verification

See [`spec/README.md`](spec/README.md):
- a TLA+ model of the protocol, model-checked with everystate (every protocol
  rule is shown necessary by a counterexample);
- a Stateright model (`crates/sidra-model`) that drives the crate's own
  protocol state machines (opening and fencing, the log, partial
  checkpoints, GC of manifests and the log) through every interleaving of
  two writers, with lost requests and responses, crashes, and requests
  that land after GC collected what they were based on. Each rule (fence, kept fences,
  `last_ts`) turned off fails a property; `tests/trace.rs` replays each
  scenario;
- Kani bounded proofs of the core functions;
- a model-based proptest of the whole system: random writes, reads and
  checkpoints, writers replaced, crashed part-way through a checkpoint, and
  fenced while still running (their writes must fail), and readers;
- the same proptest under injected faults: store requests that fail before
  applying, responses lost after applying, and random delays. Every
  acknowledged write must survive, and every read must match the model.
  (`PROPTEST_CASES=400` for stress runs.)
- deterministic simulation: the same proptest on a paused single-threaded
  tokio runtime, so virtual time drives every timer and a seed replays the
  same store requests (checked). Timers are realistic and waits let
  minutes pass, so checkpoints, purges and GC run in the background, and a
  long-lived reader checks GC keeps what it needs:
  `PROPTEST_CASES=100000 cargo test --release -p sidra --test model
  simulation`.
- targeted tests: a fenced writer's in-flight log write fails, also after GC
  collected the log around its fence; a checkpoint or claim stalled past
  the GC retention lands as a stray that GC deletes, and the live tree stays
  whole; concurrent writes on one key under
  one-batch partial checkpoints end with the newest; an overload simulation
  where writes outpace checkpoints on a slow store stall rather than fail.

## Layout

- `crates/sidra`: the library.
- `crates/sidra/tests/model.rs`: model-based proptest against a `BTreeMap`
  with tiny nodes and eager cleaning (splits, flushes, removals, relocation,
  reopen, readers). After every checkpoint it checks that the manifest's live
  bytes per segment equal the bytes of reachable nodes. Further tests cover
  fencing, GC (only referenced segments remain, space amplification is
  bounded), and crash orphans.
- `crates/sidra-model`: the Stateright model of the protocol.
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
cold scans, updates, settle, and with `--ingest-secs N` N seconds of
random overwrites with no wait between batches, printing acked and
applied MiB/s and the backlog each second. Every read is checked against
the expected value. Both engines get 640 MiB of cache: SlateDB keeps its defaults
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

Checkpoint saturation, `--latency s3`, 1M keys, then 120 s of random
overwrites from 1,024 clients (`--ingest-secs 120`), defaults otherwise
(512 MiB backlog, 64 MiB checkpoints). Before and after loading a
checkpoint's leaves together:

| | before | after |
|---|---|---|
| acked MiB/s | 22.0 | 25.3 |
| write p50 / p99 / p99.9 ms | 337 / 1,337 / 1,659 | 340 / 692 / 1,081 |
| peak unapplied | 295 MiB | 117 MiB |
| checkpoint, cold cache (~200 leaf loads) | 6–8 s | 2–3 s |
| checkpoint, warm cache | 0.8–1.2 s (keeps up) | |

The writer starts with a cold leaf cache after the reopen phases. A
checkpoint flushes a few leaves under each of dozens of inner nodes, so it
plans its flushes from the root over the cached inner nodes and runs, and
loads every leaf it will rewrite in one parallel round (about 9 ms per leaf
instead of 35). Once warm, writes can't outrun checkpoints: 37 MiB
checkpoints in about a second, mostly upload.

These tables were measured before the move to one fenced writer, with one
writer; that path is unchanged.

```sh
sidra-bench $B --latency s3 --scans 200 --reads 5000 --ingest-secs 120 --ingest-concurrency 1024
```

Bulk loading into an empty tree is limited by the writer's
copy-on-write rewrites. sidra pays for its read advantage in bytes written:
Bε buffers, plus the cleaner.
