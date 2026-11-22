use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::{Error, Result};
use crate::hlc::WriterId;
use crate::segments::SegmentTable;
use crate::store::{CreateOutcome, Store};
use crate::types::{Epoch, ManifestId, SegmentId, TreeRoot};

/// Last applied log sequence number per writer.
pub type Frontier = std::collections::BTreeMap<WriterId, u64>;

/// One published database version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Bumped by every process that takes the applier role. Publishing with
    /// put-if-absent fences the previous applier.
    pub applier_epoch: Epoch,
    /// The writer holding the applier role, or `None` once it released it
    /// (on close). Others take over when it's `None` or its lease expired.
    pub applier: Option<WriterId>,
    pub tree: Option<TreeRoot>,
    /// Writer log entries at or below these sequence numbers are in `tree`.
    pub frontier: Frontier,
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
            applier_epoch: Epoch(0),
            applier: None,
            tree: None,
            frontier: Frontier::new(),
            segment_watermark: SegmentId {
                epoch: Epoch(0),
                seq: 0,
            },
            segments: SegmentTable::new(),
            dead: Vec::new(),
            created_at_ms: now_ms,
        }
    }

    /// The same tree claimed by `by` with the next applier epoch.
    pub fn fenced(&self, by: WriterId, now_ms: u64) -> Self {
        Self {
            applier_epoch: self.applier_epoch.next(),
            applier: Some(by),
            dead: Vec::new(),
            created_at_ms: now_ms,
            ..self.clone()
        }
    }

    /// The same tree with the applier role given up.
    pub fn released(&self, now_ms: u64) -> Self {
        Self {
            applier: None,
            dead: Vec::new(),
            created_at_ms: now_ms,
            ..self.clone()
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Publish {
    Published,
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
        // Our own earlier attempt whose response was lost.
        CreateOutcome::Exists(existing) if existing == data => Ok(Publish::Published),
        CreateOutcome::Exists(_) => Ok(Publish::Conflict),
    }
}
