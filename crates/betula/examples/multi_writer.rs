//! Several writers on one database, each in its own task (they could as well
//! be separate processes or machines), plus a read-only reader.
//!
//! cargo run --example multi_writer

use std::sync::Arc;

use betula::{Db, DbReader, Freshness, ReadOptions};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> betula::Result<()> {
    // One shared store so the writers see each other; with S3 each process
    // would simply open the same URL.
    let store = Arc::new(InMemory::new());
    let mut writers = Vec::new();
    for _ in 0..3 {
        writers.push(Db::builder("shop", store.clone()).build().await?);
    }

    let tasks: Vec<_> = writers
        .into_iter()
        .enumerate()
        .map(|(w, db)| {
            tokio::spawn(async move {
                for i in 0..100 {
                    db.put(format!("order:{w}:{i:03}"), format!("writer {w}"))
                        .await?;
                }
                Ok::<_, betula::Error>(db)
            })
        })
        .collect();
    let mut writers = Vec::new();
    for t in tasks {
        writers.push(t.await.expect("writer task")?);
    }

    // Any writer, and any reader, sees every acknowledged write once it
    // pulls the others' logs.
    let latest = ReadOptions {
        freshness: Freshness::Latest,
    };
    println!(
        "order:2:099 = {:?}",
        writers[0].get_with("order:2:099", latest).await?
    );

    let reader = DbReader::builder("shop", store).build().await?;
    println!(
        "orders seen by the reader: {}",
        reader.scan("order:".."order;").collect().await?.len()
    );

    for db in writers {
        db.close().await?;
    }
    Ok(())
}
