//! One writer and a reader following it (they could as well be separate
//! processes or machines on the same bucket).
//!
//! cargo run --example follow

use std::sync::Arc;

use object_store::memory::InMemory;
use sidra::{Db, DbReader, Freshness, ReadOptions};

#[tokio::main]
async fn main() -> sidra::Result<()> {
    // With S3, each process would open the same URL instead.
    let store = Arc::new(InMemory::new());
    let db = Db::builder("shop", store.clone()).build().await?;
    let reader = DbReader::builder("shop", store.clone()).build().await?;

    for i in 0..100 {
        db.put(format!("order:{i:03}"), "placed").await?;
    }

    // A reader sees acknowledged writes once it reads the log past its
    // manifest: on its refresh timer, or now with `Freshness::Latest`.
    let latest = ReadOptions {
        freshness: Freshness::Latest,
    };
    println!(
        "order:099 = {:?}",
        reader.get_with("order:099", latest).await?
    );

    // Opening another writer fences this one: its next write fails.
    let next = Db::builder("shop", store).build().await?;
    println!("old writer: {:?}", db.put("order:100", "placed").await);
    next.put("order:100", "placed").await?;
    reader.refresh().await?;
    println!(
        "orders seen by the reader: {}",
        reader.scan("order:".."order;").collect().await?.len()
    );
    next.close().await
}
