//! Tracks the latest manifest and every writer's unapplied log tail. Shared by
//! writers (which also add their own batches directly) and readers.
//!
//! Polling uses GETs on the next expected id (manifests and log entries are
//! numbered consecutively), which cost far less than LISTs. LIST runs only on
//! discovery, to find new writers and to catch up after GC.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::hlc::WriterId;
use crate::manifest::{self, Frontier, Manifest};
use crate::memtable::Memtable;
use crate::nodes::{Nodes, Warm};
use crate::snapshot::Snapshot;
use crate::types::ManifestId;
use crate::wal::{self, WalBatch};

/// Writer log entries not yet in the tree, merged newest-wins.
#[derive(Clone, Debug, Default)]
pub struct Tails {
    batches: BTreeMap<WriterId, BTreeMap<u64, Arc<WalBatch>>>,
    mem: Memtable,
    /// When the oldest unapplied entry was first seen.
    pub since_ms: Option<u64>,
}

impl Tails {
    pub fn mem(&self) -> &Memtable {
        &self.mem
    }

    pub fn is_empty(&self) -> bool {
        self.batches.values().all(BTreeMap::is_empty)
    }

    fn add(&mut self, seq: u64, batch: Arc<WalBatch>, now_ms: u64) {
        let log = self.batches.entry(batch.writer).or_default();
        if log.contains_key(&seq) {
            return;
        }
        self.mem.apply(batch.ops.iter().cloned());
        log.insert(seq, batch);
        self.since_ms.get_or_insert(now_ms);
    }

    /// Drops entries the tree now holds.
    fn prune(&mut self, applied: &Frontier, now_ms: u64) {
        self.batches.iter_mut().for_each(|(w, log)| {
            let upto = applied.get(w).copied().unwrap_or(0);
            log.retain(|seq, _| *seq > upto);
        });
        self.batches.retain(|_, log| !log.is_empty());
        self.mem = self.batches.values().flat_map(|log| log.values()).fold(
            Memtable::default(),
            |mut mem, b| {
                mem.apply(b.ops.iter().cloned());
                mem
            },
        );
        self.since_ms = (!self.is_empty()).then_some(now_ms);
    }

    fn next_seq(&self, writer: WriterId, applied: &Frontier) -> u64 {
        let known = self
            .batches
            .get(&writer)
            .and_then(|log| log.last_key_value())
            .map(|(seq, _)| *seq);
        known.or(applied.get(&writer).copied()).unwrap_or(0) + 1
    }

    /// `applied` advanced past every entry held here.
    pub fn frontier(&self, applied: &Frontier) -> Frontier {
        let mut f = applied.clone();
        self.batches.iter().for_each(|(w, log)| {
            if let Some((seq, _)) = log.last_key_value() {
                f.insert(*w, *seq);
            }
        });
        f
    }
}

#[derive(Clone)]
pub struct View {
    pub manifest_id: ManifestId,
    pub manifest: Arc<Manifest>,
    pub tails: Tails,
}

pub struct Follower {
    nodes: Arc<Nodes>,
    rewarm: Warm,
    view: RwLock<View>,
    writers: Mutex<BTreeSet<WriterId>>,
    /// This process's own writer, whose batches arrive via `add_own`.
    own: Option<WriterId>,
    refreshing: tokio::sync::Mutex<()>,
    /// Notified whenever a new manifest is installed.
    pub installed: Notify,
}

impl Follower {
    /// `warm` loads the tree's internal nodes now; `rewarm` does it again
    /// for every new manifest (worth it only for processes serving reads).
    pub async fn open(
        nodes: Arc<Nodes>,
        warm: Warm,
        rewarm: Warm,
        own: Option<WriterId>,
    ) -> Result<Self> {
        let store = nodes.store();
        // An empty database reads as the initial manifest under id 0, which
        // any published manifest supersedes.
        let (manifest_id, manifest) = manifest::load_latest(store)
            .await?
            .unwrap_or_else(|| (ManifestId(0), Manifest::initial(0)));
        nodes.warm(manifest.tree, warm).await?;
        Ok(Self {
            nodes,
            rewarm,
            view: RwLock::new(View {
                manifest_id,
                manifest: Arc::new(manifest),
                tails: Tails::default(),
            }),
            writers: Mutex::new(BTreeSet::new()),
            own,
            refreshing: tokio::sync::Mutex::new(()),
            installed: Notify::new(),
        })
    }

    pub fn view(&self) -> View {
        self.view.read().clone()
    }

    pub fn snapshot(&self) -> Snapshot {
        let v = self.view.read();
        Snapshot {
            tails: v.tails.mem.clone(),
            tree: v.manifest.tree,
        }
    }

    pub fn tails_bytes(&self) -> usize {
        self.view.read().tails.mem.bytes()
    }

    /// Adds this process's own durable batch without waiting for a poll.
    pub fn add_own(&self, seq: u64, batch: Arc<WalBatch>, now_ms: u64) {
        let mut v = self.view.write();
        if seq > v.manifest.frontier.get(&batch.writer).copied().unwrap_or(0) {
            v.tails.add(seq, batch, now_ms);
        }
    }

    /// Installs a manifest this process published.
    pub fn install(&self, id: ManifestId, manifest: Arc<Manifest>, now_ms: u64) {
        let mut v = self.view.write();
        if id > v.manifest_id {
            v.tails.prune(&manifest.frontier, now_ms);
            v.manifest_id = id;
            v.manifest = manifest;
        }
        drop(v);
        self.installed.notify_waiters();
    }

    /// Makes later refreshes pull these writers' logs.
    pub fn track(&self, writers: impl IntoIterator<Item = WriterId>) {
        self.writers.lock().extend(writers);
    }

    /// Pulls new manifests and log entries. `discover` also LISTs, to find
    /// new writers and to catch up when consecutive probing can't. Returns
    /// whether anything new arrived.
    pub async fn refresh(&self, discover: bool, now_ms: u64) -> Result<bool> {
        let _guard = self.refreshing.lock().await;
        let store = self.nodes.store();
        if discover {
            let found = store.list_writers().await?;
            self.writers.lock().extend(found);
        }

        let current = self.view.read().manifest_id;
        let latest = match discover {
            true => store.list_manifests().await?.last().copied(),
            false => None,
        };
        let newer = match latest.filter(|id| *id > current) {
            Some(id) => load_required(store, id).await?.map(|m| (id, m)),
            None => probe_manifests(store, current).await?,
        };
        if let Some((_, m)) = &newer {
            self.nodes.warm(m.tree, self.rewarm).await?;
        }

        let (applied, starts) = {
            let v = self.view.read();
            let applied = newer
                .as_ref()
                .map_or(v.manifest.frontier.clone(), |(_, m)| m.frontier.clone());
            let starts: Vec<(WriterId, u64)> = self
                .writers
                .lock()
                .iter()
                .chain(applied.keys())
                .filter(|w| Some(**w) != self.own)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .map(|w| {
                    (
                        *w,
                        v.tails
                            .next_seq(*w, &applied)
                            .max(applied.get(w).map_or(1, |s| s + 1)),
                    )
                })
                .collect();
            (applied, starts)
        };
        let fresh = futures::future::try_join_all(
            starts
                .into_iter()
                .map(|(w, from)| read_log_from(store, w, from)),
        )
        .await?;

        let changed = newer.is_some() || fresh.iter().any(|f| !f.is_empty());
        let mut v = self.view.write();
        if let Some((id, m)) = newer.filter(|(id, _)| *id > v.manifest_id) {
            v.tails.prune(&m.frontier, now_ms);
            v.manifest_id = id;
            v.manifest = Arc::new(m);
        }
        fresh.into_iter().flatten().for_each(|(seq, batch)| {
            let upto = applied.get(&batch.writer).copied().unwrap_or(0);
            if seq > upto {
                v.tails.add(seq, batch, now_ms);
            }
        });
        drop(v);
        self.installed.notify_waiters();
        Ok(changed)
    }
}

async fn load_required(store: &crate::store::Store, id: ManifestId) -> Result<Option<Manifest>> {
    manifest::load(store, id)
        .await?
        .map(Some)
        .ok_or_else(|| Error::Corrupt {
            path: store.manifest_path(id).to_string(),
            reason: "listed manifest is missing".into(),
        })
}

/// The newest manifest after `current`, probing consecutive ids.
async fn probe_manifests(
    store: &crate::store::Store,
    current: ManifestId,
) -> Result<Option<(ManifestId, Manifest)>> {
    let mut newest = None;
    let mut id = current.next();
    while let Some(m) = manifest::load(store, id).await? {
        newest = Some((id, m));
        id = id.next();
    }
    Ok(newest)
}

/// Consecutive log entries of `writer` from `from`, stopping at the first gap.
async fn read_log_from(
    store: &crate::store::Store,
    writer: WriterId,
    from: u64,
) -> Result<Vec<(u64, Arc<WalBatch>)>> {
    let mut out = Vec::new();
    let mut seq = from;
    while let Some(batch) = wal::read(store, writer, seq).await? {
        out.push((seq, Arc::new(batch)));
        seq += 1;
    }
    Ok(out)
}
