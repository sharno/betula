use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use clap::ValueEnum;
use object_store::ObjectStore;
use slatedb::config::{FlushOptions, FlushType};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum EngineKind {
    Betula,
    Slatedb,
}

impl EngineKind {
    pub fn name(self) -> &'static str {
        match self {
            EngineKind::Betula => "betula",
            EngineKind::Slatedb => "slatedb",
        }
    }
}

#[derive(Clone, Debug)]
pub struct BetulaParams {
    pub leaf_bytes: usize,
    pub internal_bytes: usize,
    pub fanout: usize,
    pub memtable_bytes: usize,
    /// betula's split of the cache budget. SlateDB keeps its defaults:
    /// 512 MiB block cache + 128 MiB metadata cache.
    pub inner_cache_bytes: u64,
    pub leaf_cache_bytes: u64,
    pub clean_below_live: f64,
}

pub enum Engine {
    Betula(betula::Db),
    Slatedb(slatedb::Db),
}

impl Engine {
    pub async fn open(
        kind: EngineKind,
        os: Arc<dyn ObjectStore>,
        path: &str,
        params: &BetulaParams,
    ) -> Result<Self> {
        Ok(match kind {
            EngineKind::Betula => {
                let cfg = betula::DbConfig {
                    tree: betula::TreeConfig {
                        leaf_max_bytes: params.leaf_bytes,
                        internal_max_bytes: params.internal_bytes,
                        max_fanout: params.fanout,
                    },
                    cache: betula::CacheConfig {
                        inner_bytes: params.inner_cache_bytes,
                        leaf_bytes: params.leaf_cache_bytes,
                    },
                    // Same WAL flush interval as SlateDB's default.
                    wal_flush_interval: Duration::from_millis(100),
                    memtable_bytes: params.memtable_bytes,
                    cleaner: betula::CleanerConfig {
                        clean_below_live: params.clean_below_live,
                        ..betula::CleanerConfig::default()
                    },
                    ..betula::DbConfig::default()
                };
                Engine::Betula(betula::Db::open(os, path, cfg).await?)
            }
            EngineKind::Slatedb => Engine::Slatedb(slatedb::Db::builder(path, os).build().await?),
        })
    }

    /// Writes the pairs atomically; returns once the batch is durable.
    pub async fn write(&self, pairs: Vec<(Bytes, Bytes)>) -> Result<()> {
        match self {
            Engine::Betula(db) => {
                let batch = pairs
                    .into_iter()
                    .fold(betula::WriteBatch::new(), |b, (k, v)| b.put(k, v));
                db.write(batch, betula::Durability::AwaitDurable).await?
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self {
            Engine::Betula(db) => db.get(key).await?,
            Engine::Slatedb(db) => db.get(key).await?,
        })
    }

    /// Reads up to `limit` entries starting at `start`; returns the count.
    pub async fn scan(&self, start: Bytes, limit: usize) -> Result<usize> {
        let mut n = 0;
        match self {
            Engine::Betula(db) => {
                let mut it = db.scan(start..);
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
            Engine::Betula(db) => db.checkpoint().await?,
            Engine::Slatedb(db) => {
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await?
            }
        }
        Ok(())
    }

    pub fn describe(&self) -> String {
        match self {
            Engine::Betula(db) => {
                let s = db.stats();
                format!(
                    "manifest={} height={} segments={} live={:.0}%",
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
            Engine::Betula(db) => db.close().await?,
            Engine::Slatedb(db) => db.close().await?,
        }
        Ok(())
    }
}
