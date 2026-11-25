//! `ObjectStore` wrapper that counts requests and bytes, and adds simulated
//! latency (see `latency`) to approximate a remote store such as S3.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::latency::{Latency, Op};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Default, Debug)]
pub struct Counters {
    pub gets: AtomicU64,
    pub get_bytes: AtomicU64,
    pub heads: AtomicU64,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub lists: AtomicU64,
    pub deletes: AtomicU64,
    pub copies: AtomicU64,
    /// PUT bytes by object kind (see `Kind`).
    pub put_bytes_by: [AtomicU64; KINDS],
}

/// Object kinds by path, for write-amplification breakdowns.
pub const KINDS: usize = 5;
pub const KIND_NAMES: [&str; KINDS] = ["log", "segment", "manifest", "lease", "other"];

fn kind_of(path: &Path) -> usize {
    let p = path.as_ref();
    [("/wal/", 0), ("/seg/", 1), ("/manifest/", 2), ("/lease", 3)]
        .iter()
        .find(|(pat, _)| p.contains(pat))
        .map_or(4, |(_, k)| *k)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub gets: u64,
    pub get_bytes: u64,
    pub heads: u64,
    pub puts: u64,
    pub put_bytes: u64,
    pub lists: u64,
    pub deletes: u64,
    pub copies: u64,
    pub put_bytes_by: [u64; KINDS],
}

impl Counters {
    pub fn snapshot(&self) -> Snapshot {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        Snapshot {
            gets: l(&self.gets),
            get_bytes: l(&self.get_bytes),
            heads: l(&self.heads),
            puts: l(&self.puts),
            put_bytes: l(&self.put_bytes),
            lists: l(&self.lists),
            deletes: l(&self.deletes),
            copies: l(&self.copies),
            put_bytes_by: std::array::from_fn(|i| l(&self.put_bytes_by[i])),
        }
    }
}

impl std::ops::Sub for Snapshot {
    type Output = Snapshot;
    fn sub(self, o: Snapshot) -> Snapshot {
        Snapshot {
            gets: self.gets - o.gets,
            get_bytes: self.get_bytes - o.get_bytes,
            heads: self.heads - o.heads,
            puts: self.puts - o.puts,
            put_bytes: self.put_bytes - o.put_bytes,
            lists: self.lists - o.lists,
            deletes: self.deletes - o.deletes,
            copies: self.copies - o.copies,
            put_bytes_by: std::array::from_fn(|i| self.put_bytes_by[i] - o.put_bytes_by[i]),
        }
    }
}

#[derive(Debug)]
pub struct Metered {
    inner: Arc<dyn ObjectStore>,
    pub counters: Arc<Counters>,
    latency: Arc<Latency>,
}

impl Metered {
    pub fn new(inner: Arc<dyn ObjectStore>, latency: Latency) -> Self {
        Self {
            inner,
            counters: Arc::default(),
            latency: Arc::new(latency),
        }
    }

    async fn pause(&self, op: Op, bytes: u64) {
        pause(&self.latency, op, bytes).await
    }

    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

async fn pause(latency: &Latency, op: Op, bytes: u64) {
    let d = latency.delay(op, bytes);
    if !d.is_zero() {
        tokio::time::sleep(d).await;
    }
}

impl fmt::Display for Metered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metered({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Metered {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.pause(Op::Write, payload.content_length() as u64).await;
        Self::add(&self.counters.puts, 1);
        Self::add(&self.counters.put_bytes, payload.content_length() as u64);
        Self::add(
            &self.counters.put_bytes_by[kind_of(location)],
            payload.content_length() as u64,
        );
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // One PUT per part (that's how stores bill multipart uploads), with
        // the part's bytes. Each part waits its own latency.
        self.pause(Op::Write, 0).await;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(MeteredUpload {
            inner,
            counters: self.counters.clone(),
            latency: self.latency.clone(),
            kind: kind_of(location),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        let bytes = result.range.end - result.range.start;
        // Counted before the delay: a hedged copy dropped mid-delay was
        // still sent, and S3 bills it.
        if head {
            Self::add(&self.counters.heads, 1);
        } else {
            Self::add(&self.counters.gets, 1);
            Self::add(&self.counters.get_bytes, bytes);
        }
        // After the call, so the delay covers the bytes actually returned.
        self.pause(Op::Read, if head { 0 } else { bytes }).await;
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Coalesce here (as stores do) so each merged request goes through
        // `get_opts` and is counted once, with its real byte count.
        object_store::coalesce_ranges(
            ranges,
            |r| async move {
                let opts = GetOptions {
                    range: Some(r.into()),
                    ..Default::default()
                };
                self.get_opts(location, opts).await?.bytes().await
            },
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let counters = self.counters.clone();
        let counted = locations
            .inspect(move |_| Self::add(&counters.deletes, 1))
            .boxed();
        self.inner.delete_stream(counted)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        Self::add(&self.counters.lists, 1);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        Self::add(&self.counters.lists, 1);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.pause(Op::Read, 0).await;
        Self::add(&self.counters.lists, 1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.pause(Op::Write, 0).await;
        Self::add(&self.counters.copies, 1);
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct MeteredUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<Counters>,
    latency: Arc<Latency>,
    kind: usize,
}

#[async_trait]
impl MultipartUpload for MeteredUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        Metered::add(&self.counters.puts, 1);
        Metered::add(&self.counters.put_bytes, data.content_length() as u64);
        let bytes = data.content_length() as u64;
        Metered::add(&self.counters.put_bytes_by[self.kind], bytes);
        let latency = self.latency.clone();
        let part = self.inner.put_part(data);
        Box::pin(async move {
            pause(&latency, Op::Write, bytes).await;
            part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}
