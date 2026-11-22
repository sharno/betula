//! cargo run --example quickstart -- memory:///demo
//! cargo run --example quickstart -- file:///tmp/betula-demo
//! AWS_ENDPOINT=http://127.0.0.1:9100 AWS_ALLOW_HTTP=true AWS_ACCESS_KEY_ID=… \
//!   AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1 cargo run --example quickstart -- s3://bucket/demo

use betula::{Db, WriteBatch};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> betula::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "memory:///demo".into());
    let db = Db::open(&url).await?;

    db.put("greeting", "hello").await?;
    let mut batch = WriteBatch::new();
    for i in 0..5 {
        batch.put(format!("item:{i}"), format!("value {i}"));
    }
    db.write(batch).await?;

    println!("greeting = {:?}", db.get("greeting").await?);
    let mut items = db.scan("item:".."item;").into_stream();
    while let Some((k, v)) = items.try_next().await? {
        println!(
            "{} = {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }
    db.close().await
}
