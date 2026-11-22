//! `ObjectStore` wrapper that counts requests and bytes, and can add a fixed
//! per-request delay to approximate a remote store such as S3.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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
        }
    }
}

#[derive(Debug)]
pub struct Metered {
    inner: Arc<dyn ObjectStore>,
    pub counters: Arc<Counters>,
    delay: Duration,
}

impl Metered {
    pub fn new(inner: Arc<dyn ObjectStore>, delay: Duration) -> Self {
        Self {
            inner,
            counters: Arc::default(),
            delay,
        }
    }

    async fn pause(&self) {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
    }

    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
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
        self.pause().await;
        Self::add(&self.counters.puts, 1);
        Self::add(&self.counters.put_bytes, payload.content_length() as u64);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Counted as one put; part bytes are not tracked.
        self.pause().await;
        Self::add(&self.counters.puts, 1);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.pause().await;
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if head {
            Self::add(&self.counters.heads, 1);
        } else {
            Self::add(&self.counters.gets, 1);
            Self::add(
                &self.counters.get_bytes,
                result.range.end - result.range.start,
            );
        }
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
        self.pause().await;
        Self::add(&self.counters.lists, 1);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.pause().await;
        Self::add(&self.counters.copies, 1);
        self.inner.copy_opts(from, to, options).await
    }
}
