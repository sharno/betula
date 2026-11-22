//! Object layout under the database root:
//!
//! - `manifest/{id:020}.mf`: immutable manifests; the highest id is current.
//!   Writers publish with put-if-absent, which is the only coordination point.
//! - `writers/{writer}/wal/{seq:020}.wal`: each writer's log, put-if-absent.
//! - `writers/{writer}/lease`: each writer's liveness and timestamp floor.
//! - `seg/{epoch:020}-{seq:020}.seg`: one per checkpoint, holding every node
//!   that checkpoint wrote, back to back. Nodes are read with ranged GETs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::error::{Error, Result};
use crate::hlc::WriterId;
use crate::types::{Epoch, ManifestId, SegmentId};

const COALESCE_GAP: u64 = 128 << 10;
const DELETE_BATCH: usize = 128;

#[derive(Clone, Debug)]
pub struct Store {
    os: Arc<dyn ObjectStore>,
    root: Path,
    counters: Arc<Counters>,
}

/// Object store requests made by one handle.
#[derive(Debug, Default)]
struct Counters {
    gets: AtomicU64,
    get_bytes: AtomicU64,
    puts: AtomicU64,
    put_bytes: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
}

/// Object store requests made by a handle since it opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestStats {
    pub gets: u64,
    pub get_bytes: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub lists: u64,
    pub deletes: u64,
}

fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

/// Rate limit for deletes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pace {
    Unlimited,
    PerSecond(u32),
}

#[derive(Debug, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    /// Another object already exists at the path; it may be our own earlier
    /// attempt whose response was lost.
    Exists(Bytes),
}

impl Store {
    pub fn new(os: Arc<dyn ObjectStore>, root: impl Into<Path>) -> Self {
        Self {
            os,
            root: root.into(),
            counters: Arc::default(),
        }
    }

    /// Another store whose requests count toward this handle's stats.
    pub fn sibling(&self, os: Arc<dyn ObjectStore>, root: impl Into<Path>) -> Self {
        Self {
            os,
            root: root.into(),
            counters: self.counters.clone(),
        }
    }

    pub fn requests(&self) -> RequestStats {
        let c = &self.counters;
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        RequestStats {
            gets: l(&c.gets),
            get_bytes: l(&c.get_bytes),
            puts: l(&c.puts),
            put_bytes: l(&c.put_bytes),
            lists: l(&c.lists),
            deletes: l(&c.deletes),
        }
    }

    fn got(&self, b: &Bytes) {
        bump(&self.counters.gets, 1);
        bump(&self.counters.get_bytes, b.len() as u64);
    }

    fn put_counted(&self, n: usize) {
        bump(&self.counters.puts, 1);
        bump(&self.counters.put_bytes, n as u64);
    }

    fn dir(&self, name: &str) -> Path {
        self.root.clone().join(name)
    }

    pub fn manifest_path(&self, id: ManifestId) -> Path {
        self.dir("manifest").join(format!("{:020}.mf", id.0))
    }

    fn writer_dir(&self, writer: WriterId) -> Path {
        self.dir("writers").join(writer.to_string())
    }

    pub fn wal_path(&self, writer: WriterId, seq: u64) -> Path {
        self.writer_dir(writer)
            .join("wal")
            .join(format!("{seq:020}.wal"))
    }

    pub fn lease_path(&self, writer: WriterId) -> Path {
        self.writer_dir(writer).join("lease")
    }

    pub fn segment_path(&self, id: SegmentId) -> Path {
        self.dir("seg")
            .join(format!("{:020}-{:020}.seg", id.epoch.0, id.seq))
    }

    pub async fn get(&self, path: &Path) -> Result<Option<Bytes>> {
        match self.os.get(path).await {
            Ok(r) => {
                let b = r.bytes().await?;
                self.got(&b);
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_range(
        &self,
        path: &Path,
        range: std::ops::Range<u64>,
    ) -> Result<Option<Bytes>> {
        match self.os.get_range(path, range).await {
            Ok(b) => {
                self.got(&b);
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Several ranges of one object; nearby ranges are fetched together.
    pub async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Option<Vec<Bytes>>> {
        // Batched reads target inner segments (inner nodes and runs only;
        // leaves live in their own segments), so merging across gaps of dead
        // nodes costs little and saves requests.
        let fetched = object_store::coalesce_ranges(
            ranges,
            |r| async move {
                let b = self.os.get_range(path, r).await?;
                self.got(&b);
                Ok(b)
            },
            COALESCE_GAP,
        )
        .await;
        match fetched {
            Ok(b) => Ok(Some(b)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put(&self, path: &Path, data: Bytes) -> Result<()> {
        self.put_counted(data.len());
        self.os.put(path, PutPayload::from(data)).await?;
        Ok(())
    }

    /// Put-if-absent. On conflict, returns the existing object so callers can
    /// tell a retried success from a lost race.
    pub async fn put_create(&self, path: &Path, data: Bytes) -> Result<CreateOutcome> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        self.put_counted(data.len());
        match self.os.put_opts(path, PutPayload::from(data), opts).await {
            Ok(_) => Ok(CreateOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.get(path).await?.ok_or_else(|| Error::Corrupt {
                    path: path.to_string(),
                    reason: "put-if-absent conflicted but object is missing".into(),
                })?;
                Ok(CreateOutcome::Exists(existing))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes objects through the store's bulk API where it has one (S3's
    /// DeleteObjects), in small batches sent one at a time. One request per
    /// object swamps the store, and so do huge batches on some servers; either
    /// stalls writers' log PUTs behind GC. Missing objects count as deleted.
    pub async fn delete_all(&self, paths: Vec<Path>) -> Result<()> {
        self.delete_paced(paths, Pace::Unlimited).await
    }

    /// Like `delete_all`, at most `pace` objects per second: GC's bursts of
    /// deletes otherwise compete with writers' log PUTs and stall them.
    pub async fn delete_paced(&self, paths: Vec<Path>, pace: Pace) -> Result<()> {
        let (batch, gap) = match pace {
            Pace::Unlimited => (DELETE_BATCH, None),
            Pace::PerSecond(rate) => {
                let rate = rate.max(1) as usize;
                // About four batches a second.
                let batch = (rate / 4).clamp(1, DELETE_BATCH);
                (
                    batch,
                    Some(std::time::Duration::from_secs_f64(
                        batch as f64 / rate as f64,
                    )),
                )
            }
        };
        bump(&self.counters.deletes, paths.len() as u64);
        let mut paths = paths.into_iter().peekable();
        while paths.peek().is_some() {
            let chunk: Vec<Path> = paths.by_ref().take(batch).collect();
            let input = stream::iter(chunk.into_iter().map(Ok)).boxed();
            self.os
                .delete_stream(input)
                .map(|r| match r {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(Error::from(e)),
                })
                .try_collect::<Vec<()>>()
                .await?;
            if let (Some(gap), true) = (gap, paths.peek().is_some()) {
                tokio::time::sleep(gap).await;
            }
        }
        Ok(())
    }

    async fn list_names(&self, dir: &str, offset: Option<Path>) -> Result<Vec<String>> {
        bump(&self.counters.lists, 1);
        let prefix = self.dir(dir);
        let listing = match &offset {
            Some(o) => self.os.list_with_offset(Some(&prefix), o),
            None => self.os.list(Some(&prefix)),
        };
        let metas: Vec<_> = listing.try_collect().await?;
        Ok(metas
            .into_iter()
            .filter_map(|m| m.location.filename().map(str::to_string))
            .collect())
    }

    pub async fn list_manifests(&self) -> Result<Vec<ManifestId>> {
        let mut ids: Vec<_> = self
            .list_names("manifest", None)
            .await?
            .iter()
            .filter_map(|n| parse_num(n, ".mf").map(ManifestId))
            .collect();
        ids.sort();
        Ok(ids)
    }

    pub async fn list_writers(&self) -> Result<Vec<WriterId>> {
        bump(&self.counters.lists, 1);
        let listing = self
            .os
            .list_with_delimiter(Some(&self.dir("writers")))
            .await?;
        Ok(listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.filename().and_then(WriterId::parse))
            .collect())
    }

    pub async fn list_wals(&self, writer: WriterId) -> Result<Vec<u64>> {
        let prefix = self.writer_dir(writer).join("wal");
        bump(&self.counters.lists, 1);
        let metas: Vec<_> = self.os.list(Some(&prefix)).try_collect().await?;
        let mut seqs: Vec<u64> = metas
            .iter()
            .filter_map(|m| m.location.filename().and_then(|n| parse_num(n, ".wal")))
            .collect();
        seqs.sort();
        Ok(seqs)
    }

    /// Segment ids at or after `from`, sorted.
    pub async fn list_segments_from(&self, from: SegmentId) -> Result<Vec<SegmentId>> {
        let offset = from
            .seq
            .checked_sub(1)
            .map(|seq| self.segment_path(SegmentId { seq, ..from }));
        let mut ids: Vec<_> = self
            .list_names("seg", offset)
            .await?
            .iter()
            .filter_map(|n| parse_segment(n))
            .filter(|id| *id >= from)
            .collect();
        ids.sort();
        Ok(ids)
    }
}

fn parse_num(name: &str, suffix: &str) -> Option<u64> {
    name.strip_suffix(suffix)?.parse().ok()
}

fn parse_segment(name: &str) -> Option<SegmentId> {
    let (epoch, seq) = name.strip_suffix(".seg")?.split_once('-')?;
    Some(SegmentId {
        epoch: Epoch(epoch.parse().ok()?),
        seq: seq.parse().ok()?,
    })
}
