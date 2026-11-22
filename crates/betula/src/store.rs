//! Object layout under the database root:
//!
//! - `manifest/{id:020}.mf`: immutable manifests; the highest id is current.
//!   Writers publish with put-if-absent, which is the only coordination point.
//! - `wal/{id:020}.wal`: write-ahead log batches, also put-if-absent.
//! - `seg/{epoch:020}-{seq:020}.seg`: one per checkpoint, holding every node
//!   that checkpoint wrote, back to back. Nodes are read with ranged GETs.

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::error::{Error, Result};
use crate::types::{Epoch, ManifestId, SegmentId, WalId};

const DELETE_CONCURRENCY: usize = 32;

#[derive(Clone, Debug)]
pub struct Store {
    os: Arc<dyn ObjectStore>,
    root: Path,
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
        }
    }

    fn dir(&self, name: &str) -> Path {
        self.root.clone().join(name)
    }

    pub fn manifest_path(&self, id: ManifestId) -> Path {
        self.dir("manifest").join(format!("{:020}.mf", id.0))
    }

    pub fn wal_path(&self, id: WalId) -> Path {
        self.dir("wal").join(format!("{:020}.wal", id.0))
    }

    pub fn segment_path(&self, id: SegmentId) -> Path {
        self.dir("seg")
            .join(format!("{:020}-{:020}.seg", id.epoch.0, id.seq))
    }

    pub async fn get(&self, path: &Path) -> Result<Option<Bytes>> {
        match self.os.get(path).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
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
            Ok(b) => Ok(Some(b)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn put(&self, path: &Path, data: Bytes) -> Result<()> {
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

    pub async fn delete_all(&self, paths: Vec<Path>) -> Result<()> {
        stream::iter(paths)
            .map(|p| async move {
                match self.os.delete(&p).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(Error::from(e)),
                }
            })
            .buffer_unordered(DELETE_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn list_names(&self, dir: &str, offset: Option<Path>) -> Result<Vec<String>> {
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

    pub async fn list_wals(&self) -> Result<Vec<WalId>> {
        let mut ids: Vec<_> = self
            .list_names("wal", None)
            .await?
            .iter()
            .filter_map(|n| parse_num(n, ".wal").map(WalId))
            .collect();
        ids.sort();
        Ok(ids)
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
