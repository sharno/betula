//! Raw PUT/GET throughput of the store at a few object sizes and concurrencies,
//! to tell engine costs apart from store limits.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

#[tokio::main]
async fn main() -> Result<()> {
    let os: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint("http://127.0.0.1:9100")
            .with_bucket_name("bench")
            .with_access_key_id("betula")
            .with_secret_access_key("betula-secret")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()?,
    );
    println!("| op | size KiB | conc | objects | obj/s | MiB/s |");
    println!("|---|---:|---:|---:|---:|---:|");
    for (size_kib, count) in [(4usize, 2000usize), (64, 1000), (1024, 200), (16384, 16)] {
        for conc in [1usize, 16, 64] {
            let data = Bytes::from(vec![7u8; size_kib << 10]);
            let paths: Vec<Path> = (0..count)
                .map(|i| Path::from(format!("probe/{size_kib}-{conc}/{i}")))
                .collect();
            let t = Instant::now();
            stream::iter(paths.clone())
                .map(|p| {
                    let (os, d) = (os.clone(), data.clone());
                    async move { os.put(&p, PutPayload::from(d)).await }
                })
                .buffer_unordered(conc)
                .try_collect::<Vec<_>>()
                .await?;
            report("PUT", size_kib, conc, count, t.elapsed().as_secs_f64());
            let t = Instant::now();
            stream::iter(paths.clone())
                .map(|p| {
                    let os = os.clone();
                    async move { os.get(&p).await?.bytes().await }
                })
                .buffer_unordered(conc)
                .try_collect::<Vec<_>>()
                .await?;
            report("GET", size_kib, conc, count, t.elapsed().as_secs_f64());
            stream::iter(paths)
                .map(|p| {
                    let os = os.clone();
                    async move { os.delete(&p).await }
                })
                .buffer_unordered(64)
                .try_collect::<Vec<_>>()
                .await?;
        }
    }
    Ok(())
}

fn report(op: &str, size_kib: usize, conc: usize, count: usize, secs: f64) {
    println!(
        "| {op} | {size_kib} | {conc} | {count} | {:.0} | {:.1} |",
        count as f64 / secs,
        (count * size_kib) as f64 / 1024.0 / secs
    );
}
