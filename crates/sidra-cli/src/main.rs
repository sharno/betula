//! `sidra`: inspect and maintain sidra databases.
//!
//! ```sh
//! sidra info s3://bucket/db
//! sidra log s3://bucket/db
//! sidra get s3://bucket/db user:1
//! sidra scan s3://bucket/db --prefix user: --limit 20
//! sidra put s3://bucket/db user:1 ada --take-over
//! sidra gc s3://bucket/db --retention 10m
//! ```
//!
//! Credentials come from AWS_* / GOOGLE_* / AZURE_*, settings from
//! SIDRA_CONFIG and SIDRA_*.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::{Parser, Subcommand};
use humantime_serde::re::humantime;
use sidra::{Admin, Db, DbReader, Freshness, ReadOptions, Settings};

#[derive(Parser)]
#[command(
    name = "sidra",
    about = "Inspect and maintain sidra databases",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Current manifest, tree and space usage.
    Info { url: String },
    /// Log objects: stored, unapplied, and fences.
    Log { url: String },
    /// Walks the whole tree (reads every node) and prints its shape.
    Shape { url: String },
    /// Prints a value, the writer's latest acknowledged writes included.
    Get { url: String, key: String },
    /// Prints pairs in key order.
    Scan {
        url: String,
        /// Only keys starting with this.
        #[arg(long)]
        prefix: Option<String>,
        /// Only keys at or after this.
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Writes a value through a short-lived writer.
    Put {
        url: String,
        key: String,
        value: String,
        #[command(flatten)]
        take_over: TakeOver,
    },
    /// Deletes a key through a short-lived writer.
    Delete {
        url: String,
        key: String,
        #[command(flatten)]
        take_over: TakeOver,
    },
    /// Deletes data no reader within the retention can need.
    Gc {
        url: String,
        #[arg(long, default_value = "10m", value_parser = humantime::parse_duration)]
        retention: Duration,
    },
}

/// Opening a writer fences the running one, so edits must say so.
#[derive(clap::Args)]
struct TakeOver {
    /// Fence the database's current writer (it fails from its next write
    /// or checkpoint on, until reopened).
    #[arg(long, required = true)]
    take_over: bool,
}

fn ago(ms: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let d = Duration::from_millis(now.saturating_sub(ms) / 1000 * 1000);
    format!("{} ago", humantime::format_duration(d))
}

fn mib(b: u64) -> String {
    format!("{:.1} MiB", b as f64 / (1 << 20) as f64)
}

fn show(b: &[u8]) -> String {
    String::from_utf8(b.to_vec()).unwrap_or_else(|_| format!("{b:02x?}"))
}

/// The smallest key greater than every key starting with `prefix`.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// A writer for one-off edits. It fences the current writer.
async fn edit_writer(url: &str) -> Result<Db> {
    let (store, path) = sidra::parse_url(url)?;
    let settings = Settings {
        warm: sidra::Warm::Off,
        ..Settings::load()?
    };
    Ok(Db::builder(path, store)
        .with_settings(settings)
        .build()
        .await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Info { url } => {
            let admin = Admin::open(&url)?;
            let Some(m) = admin.manifest().await? else {
                println!("empty database (no manifest)");
                return Ok(());
            };
            println!("manifest        {} ({})", m.id.0, ago(m.created_at_ms));
            println!("writer epoch    {}", m.writer_epoch.0);
            match m.tree {
                Some(t) => println!("tree            height {}", t.height),
                None => println!("tree            empty"),
            }
            let live = 100.0 * m.live_bytes as f64 / m.segment_bytes.max(1) as f64;
            println!(
                "segments        {} ({}, {live:.0}% live)",
                m.segments,
                mib(m.segment_bytes)
            );
            println!("log applied     {}", m.wal_applied);
        }
        Cmd::Log { url } => {
            let l = Admin::open(&url)?.log().await?;
            println!("objects         {}", l.objects);
            println!("unapplied       {}", l.unapplied);
            println!("fences          {}", l.fences);
        }
        Cmd::Shape { url } => {
            let s = Admin::open(&url)?.tree_shape().await?;
            println!(
                "internal {}  leaves {}  runs {}  tombstones {}",
                s.internal, s.leaves, s.runs, s.tombstones
            );
        }
        Cmd::Get { url, key } => {
            let reader = DbReader::open(&url).await?;
            let latest = ReadOptions {
                freshness: Freshness::Latest,
            };
            match reader.get_with(&key, latest).await? {
                Some(v) => println!("{}", show(&v)),
                None => {
                    eprintln!("not found");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Scan {
            url,
            prefix,
            from,
            limit,
        } => {
            let reader = DbReader::open(&url).await?;
            let start = match (&from, &prefix) {
                (Some(f), _) => f.clone().into_bytes(),
                (None, Some(p)) => p.clone().into_bytes(),
                (None, None) => Vec::new(),
            };
            let end = prefix.as_deref().and_then(|p| prefix_end(p.as_bytes()));
            let mut it = match end {
                Some(end) => reader.scan(start..end),
                None => reader.scan(start..),
            };
            let mut n = 0;
            while n < limit {
                let Some((k, v)) = it.next().await? else {
                    break;
                };
                println!("{}\t{}", show(&k), show(&v));
                n += 1;
            }
        }
        Cmd::Put {
            url, key, value, ..
        } => {
            let db = edit_writer(&url).await?;
            db.put(&key, &value).await?;
            db.close().await?;
        }
        Cmd::Delete { url, key, .. } => {
            let db = edit_writer(&url).await?;
            db.delete(&key).await?;
            db.close().await?;
        }
        Cmd::Gc { url, retention } => {
            let s = Admin::open(&url)?.gc(retention).await?;
            println!(
                "deleted {} manifests, {} segments, {} log objects",
                s.manifests, s.segments, s.wals
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn prefix_end_increments_last_byte() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(&[b'a', 0xff]), Some(b"b".to_vec()));
        assert_eq!(prefix_end(&[0xff]), None);
    }
}
