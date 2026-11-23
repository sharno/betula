//! Warm range-scan throughput on an in-memory store, for profiling scans
//! without object-store noise:
//!     cargo run --release --example scan_profile -- [keys] [scan_len] [secs] [tasks]
//! Loads `keys` keys through regular checkpoints (so runs sit above leaves),
//! then runs `scan_len`-key scans from random starts for `secs` seconds on
//! `tasks` concurrent tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use sidra::{Db, Settings, WriteBatch};

fn arg(i: usize, default: u64) -> u64 {
    std::env::args()
        .nth(i)
        .and_then(|a| a.parse().ok())
        .unwrap_or(default)
}

fn key(id: u64) -> String {
    format!("key{id:012}")
}

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[tokio::main]
async fn main() -> sidra::Result<()> {
    let (keys, len, secs, tasks) = (arg(1, 200_000), arg(2, 100), arg(3, 5), arg(4, 1));
    let db = Arc::new(
        Db::builder("bench", Arc::new(InMemory::new()))
            .with_settings(Settings::default())
            .build()
            .await?,
    );
    let value = vec![7u8; 256];
    // Random insertion order, in batches, with a checkpoint every so often:
    // leaves end up with runs above them, as in steady state.
    for chunk in 0..keys.div_ceil(1000) {
        let mut batch = WriteBatch::new();
        (chunk * 1000..((chunk + 1) * 1000).min(keys)).for_each(|i| {
            batch.put(key(mix(i) % keys), &value);
        });
        db.write(batch).await?;
        if chunk % 20 == 19 {
            db.checkpoint().await?;
        }
    }
    db.flush_wal().await?;
    let m = db.metrics();
    println!(
        "loaded {keys} keys, height {:?}, unapplied {} B",
        m.tree.map(|t| t.height),
        m.unapplied_bytes
    );

    let done = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(secs);
    let started = Instant::now();
    let handles: Vec<_> = (0..tasks)
        .map(|t| {
            let (db, done) = (db.clone(), done.clone());
            tokio::spawn(async move {
                let mut i = t << 40;
                while Instant::now() < deadline {
                    let start = mix(i) % keys.saturating_sub(len * 2).max(1);
                    let mut it = db.scan(key(start)..);
                    let mut n = 0;
                    while n < len && it.next().await?.is_some() {
                        n += 1;
                    }
                    i += 1;
                    done.fetch_add(1, Ordering::Relaxed);
                }
                sidra::Result::Ok(())
            })
        })
        .collect();
    for h in handles {
        h.await.expect("scan task")?;
    }
    let n = done.load(Ordering::Relaxed);
    println!(
        "{n} scans of {len} in {:.1}s: {:.0} scans/s ({tasks} tasks)",
        started.elapsed().as_secs_f64(),
        n as f64 / started.elapsed().as_secs_f64()
    );
    Arc::try_unwrap(db).ok().expect("db shared").close().await
}
