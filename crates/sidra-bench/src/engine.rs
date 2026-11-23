use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use clap::ValueEnum;
use object_store::ObjectStore;
use slatedb::config::{FlushOptions, FlushType};

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
pub struct SidraParams {
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
    pub compression: crate::CompressionArg,
    pub warm: sidra::Warm,
}

pub enum Engine {
    /// Several concurrent writers on one database. The first opened takes
    /// the applier role and serves reads, so its cache is the warm one.
    Sidra(Vec<sidra::Db>),
    Slatedb(slatedb::Db),
}

impl Engine {
    pub async fn open(
        kind: EngineKind,
        os: Arc<dyn ObjectStore>,
        path: &str,
        params: &SidraParams,
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
                dbs[slot as usize % dbs.len()].write(batch).await?
            }
            Engine::Slatedb(db) => {
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
                dbs[slot as usize % dbs.len()].write(batch).await?
            }
            Engine::Slatedb(db) => {
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
    pub async fn shape(&self) -> Result<Option<String>> {
        Ok(match self {
            Engine::Sidra(dbs) => {
                let s = dbs[0].tree_shape().await?;
                Some(format!(
                    "internal {} leaves {} runs {} tombstones {}",
                    s.internal, s.leaves, s.runs, s.tombstones
                ))
            }
            Engine::Slatedb(_) => None,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self {
            Engine::Sidra(dbs) => dbs[0].get(key).await?,
            Engine::Slatedb(db) => db.get(key).await?,
        })
    }

    /// Reads up to `limit` entries starting at `start`; returns the count.
    pub async fn scan(&self, start: Bytes, limit: usize) -> Result<usize> {
        let mut n = 0;
        match self {
            Engine::Sidra(dbs) => {
                let mut it = dbs[0].scan(start..);
                while n < limit && it.next().await?.is_some() {
                    n += 1;
                }
            }
            Engine::Slatedb(db) => {
                let mut it = db.scan(start..).await?;
                while n < limit && it.next().await?.is_some() {
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Moves everything written into the engine's long-term structure
    /// (tree checkpoint / L0 flush).
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
            Engine::Slatedb(db) => {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?
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
            Engine::Slatedb(_) => String::new(),
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
            Engine::Slatedb(db) => db.close().await?,
        }
        Ok(())
    }
}
