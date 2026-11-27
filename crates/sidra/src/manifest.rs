use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::{Error, Result};
use crate::hlc::Ts;
use crate::segments::SegmentTable;
use crate::store::{CreateOutcome, Store};
use crate::types::{Epoch, ManifestId, SegmentId, TreeRoot, WalSeq, WriterId};

/// One published database version.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Manifest {
    /// Bumped by every writer that opens the database. Publishing with
    /// put-if-absent fences the previous writer's checkpoints; its fence in
    /// the log fences its writes.
    pub writer_epoch: Epoch,
    /// The opening that published this manifest: its claim and checkpoints.
    pub writer: WriterId,
    /// The opening that published the manifest this one replaced. A writer
    /// stalled past the GC retention can put a manifest into an id GC
    /// already collected; the manifest above it then names another parent,
    /// and GC deletes the stray without acting on it.
    pub parent: WriterId,
    pub tree: Option<TreeRoot>,
    /// The newest timestamp applied. A new writer's clock starts past it and
    /// past the log it replays, so timestamps grow across writers even when
    /// the versions that carried them are gone.
    pub last_ts: Ts,
    /// Log slots up to here are in `tree`.
    pub wal_applied: WalSeq,
    /// Every fence at or below `wal_applied`. GC never deletes them: a
    /// fenced writer may still have a put for that slot in flight, which
    /// would land on an empty slot and be acknowledged without anyone ever
    /// reading it.
    pub fences: BTreeSet<WalSeq>,
    /// First segment id the publishing writer had not used. Segments at or
    /// above it that exist in storage are orphans of a writer that died
    /// before publishing.
    pub segment_watermark: SegmentId,
    /// Every segment this tree references, with its live bytes.
    pub segments: SegmentTable,
    /// Segments the previous tree referenced and this one doesn't. Deleted by
    /// GC once this manifest is past retention.
    pub dead: Vec<SegmentId>,
    pub created_at_ms: u64,
}

impl Manifest {
    pub fn initial(now_ms: u64) -> Self {
        Self {
            writer_epoch: Epoch(0),
            writer: WriterId(0),
            parent: WriterId(0),
            tree: None,
            last_ts: Ts::MIN,
            wal_applied: WalSeq::ZERO,
            fences: BTreeSet::new(),
            segment_watermark: SegmentId {
                epoch: Epoch(0),
                seq: 0,
            },
            segments: SegmentTable::new(),
            dead: Vec::new(),
            created_at_ms: now_ms,
        }
    }

    /// The same tree claimed by opening `writer`, the next generation.
    pub fn claimed(&self, writer: WriterId, now_ms: u64) -> Self {
        Self {
            writer_epoch: self.writer_epoch.next(),
            writer,
            parent: self.writer,
            dead: Vec::new(),
            created_at_ms: now_ms,
            ..self.clone()
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Publish {
    Published,
    /// The id holds this exact manifest already: our own earlier attempt
    /// whose response was lost, or another writer's identical one.
    Matched,
    /// Another writer published this id first.
    Conflict,
}

pub async fn load(store: &Store, id: ManifestId) -> Result<Option<Manifest>> {
    let path = store.manifest_path(id);
    store
        .get(&path)
        .await?
        .map(|data| codec::decode(codec::MANIFEST, path.as_ref(), &data))
        .transpose()
}

pub async fn load_latest(store: &Store) -> Result<Option<(ManifestId, Manifest)>> {
    let Some(id) = store.list_manifests().await?.last().copied() else {
        return Ok(None);
    };
    // The latest manifest is never garbage collected.
    let manifest = load(store, id).await?.ok_or_else(|| Error::Corrupt {
        path: store.manifest_path(id).to_string(),
        reason: "listed manifest is missing".into(),
    })?;
    Ok(Some((id, manifest)))
}

pub async fn publish(store: &Store, id: ManifestId, manifest: &Manifest) -> Result<Publish> {
    // Manifests carry the segment table, which compresses well.
    let data = codec::encode(codec::MANIFEST, manifest, codec::Compression::Lz4);
    match store
        .put_create(&store.manifest_path(id), data.clone())
        .await?
    {
        CreateOutcome::Created => Ok(Publish::Published),
        CreateOutcome::Exists(existing) if existing == data => Ok(Publish::Matched),
        CreateOutcome::Exists(_) => Ok(Publish::Conflict),
    }
}
