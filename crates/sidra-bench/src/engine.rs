use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use clap::ValueEnum;
use object_store::ObjectStore;
use slatedb::config::{FlushOptions, FlushType, ScanOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineKind {
    Sidra,
    Slatedb,
}

impl EngineKind {
    pub fn name(self) -> &'static str {
        match self {
            EngineKind::Sidra => "sidra",
            EngineKind::Slatedb => "slatedb",
        }
    }
}

#[derive(Clone, Debug)]
pub struct EngineParams {
    pub leaf_bytes: usize,
    pub internal_bytes: usize,
    pub fanout: usize,
    pub memtable_bytes: usize,
    /// sidra's split of the cache budget. SlateDB keeps its defaults:
    /// 512 MiB block cache + 128 MiB metadata cache.
    pub inner_cache_bytes: u64,
    pub leaf_cache_bytes: u64,
    pub clean_below_live: f64,
    pub clean_inner_below_live: f64,
    /// GC delete rate, or `None` to disable GC.
    pub gc: Option<u32>,
    /// How often sidra writers poll for each other's entries.
    pub poll: Duration,
    pub max_runs: usize,
    /// sidra: how long tombstones wait before a checkpoint rewrites their
    /// subtree just to drop them.
    pub purge_after: Duration,
    pub compression: crate::CompressionArg,
    pub warm: sidra::Warm,
    pub hedge: sidra::Hedge,
    /// How SlateDB scans fetch and cache blocks.
    pub slatedb_scan: ScanOptions,
}

/// How long SlateDB's manifest must stay unchanged to count as settled.
const SETTLE_QUIET: Duration = Duration::from_secs(10);
const SETTLE_LIMIT: Duration = Duration::from_secs(600);

/// Returns once SlateDB's manifest stops changing: its background compaction
/// is done. Without this, later phases race a compaction that rewrites every
/// SST, and SlateDB doesn't cache compaction output, so "warm" reads go to
/// the store.
async fn compaction_done(db: &slatedb::Db) -> Result<()> {
    let mut last = db.manifest().id();
    loop {
        tokio::time::sleep(SETTLE_QUIET).await;
        db.refresh_manifest().await?;
        let id = db.manifest().id();
        if id == last {
            return Ok(());
        }
        last = id;
    }
}

/// Which scan settings a SlateDB scan uses.
#[derive(Clone, Copy)]
enum Pass {
    /// The settings under test.
    Measured,
    /// Large fetches, for the unmeasured warm-up.
    Warmup,
}

const WARMUP_READ_AHEAD: usize = 1 << 20;
const WARMUP_FETCH_TASKS: usize = 8;

pub enum Engine {
    /// Several concurrent writers on one database. The first opened takes
    /// the applier role and serves reads, so its cache is the warm one.
    Sidra(Vec<sidra::Db>),
    Slatedb(slatedb::Db, ScanOptions),
}

impl Engine {
    pub async fn open(
        kind: EngineKind,
        os: Arc<dyn ObjectStore>,
        path: &str,
        params: &EngineParams,
        writers: usize,
    ) -> Result<Self> {
        Ok(match kind {
            EngineKind::Sidra => {
                let defaults = sidra::Settings::default();
                let cfg = sidra::Settings {
                    tree: sidra::TreeConfig {
                        leaf_max_bytes: params.leaf_bytes,
                        internal_max_bytes: params.internal_bytes,
                        max_fanout: params.fanout,
                        max_runs_per_child: params.max_runs,
                        purge_tombstones_after: params.purge_after,
                        purge_bytes_per_checkpoint: defaults.tree.purge_bytes_per_checkpoint,
                    },
                    cache: sidra::CacheConfig {
                        inner_bytes: params.inner_cache_bytes,
                        leaf_bytes: params.leaf_cache_bytes,
                    },
                    // Same WAL flush interval as SlateDB's default.
                    wal: sidra::WalConfig {
                        flush_interval: Duration::from_millis(100),
                        ..defaults.wal
                    },
                    checkpoint: sidra::CheckpointConfig {
                        bytes: params.memtable_bytes,
                        ..defaults.checkpoint
                    },
                    poll: sidra::PollConfig {
                        interval: params.poll,
                        ..defaults.poll
                    },
                    warm: params.warm,
                    hedge: params.hedge,
                    gc: match params.gc {
                        Some(rate) => sidra::Gc::On(sidra::GcConfig {
                            max_deletes_per_sec: rate,
                            ..sidra::GcConfig::default()
                        }),
                        None => sidra::Gc::Off,
                    },
                    compression: match params.compression {
                        crate::CompressionArg::None => sidra::Compression::None,
                        crate::CompressionArg::Lz4 => sidra::Compression::Lz4,
                    },
                    cleaner: sidra::CleanerConfig {
                        clean_below_live: params.clean_below_live,
                        clean_inner_below_live: params.clean_inner_below_live,
                        ..sidra::CleanerConfig::default()
                    },
                    ..defaults
                };
                let mut dbs = Vec::with_capacity(writers);
                for _ in 0..writers.max(1) {
                    dbs.push(
                        sidra::Db::builder(path, os.clone())
                            .with_settings(cfg.clone())
                            .build()
                            .await?,
                    );
                }
                Engine::Sidra(dbs)
            }
            EngineKind::Slatedb => {
                let settings = slatedb::Settings {
                    compression_codec: match params.compression {
                        crate::CompressionArg::None => None,
                        crate::CompressionArg::Lz4 => Some(slatedb::config::CompressionCodec::Lz4),
                    },
                    ..slatedb::Settings::default()
                };
                Engine::Slatedb(
                    slatedb::Db::builder(path, os)
                        .with_settings(settings)
                        .build()
                        .await?,
                    params.slatedb_scan.clone(),
                )
            }
        })
    }

    /// Writes the pairs atomically through writer `slot` (modulo the writer
    /// count); returns once the batch is durable.
    pub async fn write(&self, slot: u64, pairs: Vec<(Bytes, Bytes)>) -> Result<()> {
        match self {
            Engine::Sidra(dbs) => {
                let mut batch = sidra::WriteBatch::new();
                pairs.into_iter().for_each(|(k, v)| {
                    batch.put_bytes(k, v);
                });
                dbs[slot as usize % dbs.len()]
                    .write(batch)
                    .await
                    .map(drop)?
            }
            Engine::Slatedb(db, _) => {
                let batch = pairs
                    .into_iter()
                    .fold(slatedb::WriteBatch::new(), |mut b, (k, v)| {
                        b.put_bytes(k, v);
                        b
                    });
                db.write(batch).await?.await_durable().await?
            }
        }
        Ok(())
    }

    /// Deletes the keys atomically through writer `slot`.
    pub async fn delete(&self, slot: u64, keys: Vec<Bytes>) -> Result<()> {
        match self {
            Engine::Sidra(dbs) => {
                let mut batch = sidra::WriteBatch::new();
                keys.iter().for_each(|k| {
                    batch.delete(k);
                });
                dbs[slot as usize % dbs.len()]
                    .write(batch)
                    .await
                    .map(drop)?
            }
            Engine::Slatedb(db, _) => {
                let batch = keys.iter().fold(slatedb::WriteBatch::new(), |mut b, k| {
                    b.delete(k);
                    b
                });
                db.write(batch).await?.await_durable().await?
            }
        }
        Ok(())
    }

    /// Tree shape (sidra only): walks every node.
    /// Also leaf fill against `leaf_max`: leaves below a quarter, mean fill,
    /// and how many leaves packing adjacent ones (no splits) would leave.
    pub async fn shape(&self, leaf_max: usize) -> Result<Option<String>> {
        Ok(match self {
            Engine::Sidra(dbs) => {
                let s = dbs[0].tree_shape().await?;
                let underfull = s.leaf_bytes.iter().filter(|b| **b < leaf_max / 4).count();
                let fill = s.leaf_bytes.iter().sum::<usize>() as f64
                    / (s.leaf_bytes.len().max(1) * leaf_max) as f64;
                let packed = s
                    .leaf_bytes
                    .iter()
                    .fold((0usize, usize::MAX), |(n, open), b| {
                        match open.checked_add(*b) {
                            Some(sum) if sum <= leaf_max => (n, sum),
                            _ => (n + 1, *b),
                        }
                    })
                    .0;
                Some(format!(
                    "internal {} leaves {} runs {} tombstones {} underfull leaves {} mean fill {:.0}% packed leaves {}",
                    s.internal,
                    s.leaves,
                    s.runs,
                    s.tombstones,
                    underfull,
                    100.0 * fill,
                    packed
                ))
            }
            Engine::Slatedb(..) => None,
        })
    }

    /// SlateDB's default `ReadOptions` already match sidra: fetched blocks are
    /// cached, and bloom filters (10 bits per key) skip SSTs without the key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self {
            Engine::Sidra(dbs) => dbs[0].get(key).await?,
            Engine::Slatedb(db, _) => db.get(key).await?,
        })
    }

    /// Reads up to `limit` entries starting at `start`; returns the count.
    pub async fn scan(&self, start: Bytes, limit: usize) -> Result<usize> {
        self.scan_pass(start, limit, Pass::Measured).await
    }

    /// An unmeasured scan that fills the caches. SlateDB fetches in large
    /// blocks here, so warming up doesn't take as long as the scan settings
    /// under test would (one 4 KiB block per GET is minutes at 20 ms).
    pub async fn warm_up(&self, start: Bytes, limit: usize) -> Result<usize> {
        self.scan_pass(start, limit, Pass::Warmup).await
    }

    async fn scan_pass(&self, start: Bytes, limit: usize, pass: Pass) -> Result<usize> {
        let mut n = 0;
        match self {
            Engine::Sidra(dbs) => {
                let mut it = dbs[0].scan(start..);
                while n < limit && it.next().await?.is_some() {
                    n += 1;
                }
            }
            Engine::Slatedb(db, measured) => {
                let options = match pass {
                    Pass::Measured => measured.clone(),
                    Pass::Warmup => ScanOptions::default()
                        .with_cache_blocks(true)
                        .with_read_ahead_bytes(WARMUP_READ_AHEAD)
                        .with_max_fetch_tasks(WARMUP_FETCH_TASKS),
                };
                let mut it = db.scan_with_options(start.., &options).await?;
                while n < limit && it.next().await?.is_some() {
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Settles until due tombstones are gone. sidra purges at most
    /// `purge_bytes_per_checkpoint` per checkpoint, so it checkpoints until
    /// one leaves the tree as it was.
    pub async fn purge(&self) -> Result<()> {
        let root = || match self {
            Engine::Sidra(dbs) => dbs
                .iter()
                .find(|d| d.is_applier())
                .and_then(|d| d.metrics().tree),
            Engine::Slatedb(..) => None,
        };
        let mut before = root();
        loop {
            self.settle().await?;
            let after = root();
            if after == before {
                return Ok(());
            }
            before = after;
        }
    }

    /// Moves everything written into the engine's long-term structure (tree
    /// checkpoint / L0 flush) and waits for background compaction to finish.
    pub async fn settle(&self) -> Result<()> {
        match self {
            Engine::Sidra(dbs) => {
                for db in dbs {
                    db.flush_wal().await?;
                }
                let applier = match dbs.iter().find(|d| d.is_applier()) {
                    Some(d) => d,
                    None => {
                        dbs[0].claim_applier().await?;
                        &dbs[0]
                    }
                };
                applier.checkpoint().await?
            }
            Engine::Slatedb(db, _) => {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?;
                tokio::time::timeout(SETTLE_LIMIT, compaction_done(db))
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("SlateDB still compacting after {SETTLE_LIMIT:?}")
                    })??
            }
        }
        Ok(())
    }

    /// Time from a durable write on one writer until another writer reads it.
    /// `None` for single-writer engines.
    pub async fn visibility_lag(&self, key: Bytes, value: Bytes) -> Result<Option<Duration>> {
        let Engine::Sidra(dbs) = self else {
            return Ok(None);
        };
        let [reader, writer, ..] = dbs.as_slice() else {
            return Ok(None);
        };
        writer.put(key.clone(), value.clone()).await?;
        let start = std::time::Instant::now();
        while reader.get(&key).await?.as_ref() != Some(&value) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(Some(start.elapsed()))
    }

    pub fn describe(&self) -> String {
        match self {
            Engine::Sidra(dbs) => {
                let s = dbs[0].metrics();
                format!(
                    "writers={} manifest={} height={} segments={} live={:.0}%",
                    dbs.len(),
                    s.manifest_id.0,
                    s.tree.map_or(0, |t| t.height),
                    s.segments,
                    100.0 * s.live_bytes as f64 / s.segment_bytes.max(1) as f64
                )
            }
            Engine::Slatedb(..) => String::new(),
        }
    }

    pub async fn close(self) -> Result<()> {
        match self {
            Engine::Sidra(dbs) => {
                // Non-appliers first, so the applier's final checkpoint sees
                // everything.
                let (applier, rest): (Vec<_>, Vec<_>) =
                    dbs.into_iter().partition(|d| d.is_applier());
                for db in rest.into_iter().chain(applier) {
                    db.close().await?;
                }
            }
            Engine::Slatedb(db, _) => db.close().await?,
        }
        Ok(())
    }
}
