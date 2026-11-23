//! `ObjectStore` wrapper that injects faults: requests that fail before
//! reaching the store, requests whose response is lost after the store
//! applied them, and random delays that reorder concurrent requests.
//! Deterministic per seed (up to task scheduling).

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Probabilities per request.
#[derive(Clone, Copy, Debug)]
pub struct Faults {
    /// The request fails without reaching the store.
    pub fail_before: f64,
    /// The store applies a write, but the response is lost.
    pub fail_after: f64,
    pub max_delay: Duration,
}

#[derive(Debug)]
pub struct Faulty {
    inner: Arc<dyn ObjectStore>,
    faults: Faults,
    rng: Arc<AtomicU64>,
    enabled: Arc<AtomicBool>,
    pub injected: Arc<AtomicU64>,
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn injected(what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "faulty",
        source: format!("injected fault: {what}").into(),
    }
}

impl Faulty {
    pub fn new(inner: Arc<dyn ObjectStore>, faults: Faults, seed: u64) -> Self {
        Self {
            inner,
            faults,
            rng: Arc::new(AtomicU64::new(seed)),
            enabled: Arc::new(AtomicBool::new(true)),
            injected: Arc::default(),
        }
    }

    /// Turns faults on or off (for fault-free verification at the end).
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::SeqCst);
    }

    fn roll(&self) -> f64 {
        let r = splitmix(self.rng.fetch_add(1, Ordering::Relaxed));
        (r >> 11) as f64 / (1u64 << 53) as f64
    }

    fn hit(&self, p: f64) -> bool {
        let hit = self.enabled.load(Ordering::Relaxed) && self.roll() < p;
        if hit {
            self.injected.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    async fn before(&self, what: &str) -> Result<()> {
        if self.enabled.load(Ordering::Relaxed) && !self.faults.max_delay.is_zero() {
            tokio::time::sleep(self.faults.max_delay.mul_f64(self.roll())).await;
        }
        match self.hit(self.faults.fail_before) {
            true => Err(injected(what)),
            false => Ok(()),
        }
    }

    fn after<T>(&self, what: &str, r: Result<T>) -> Result<T> {
        match r {
            Ok(_) if self.hit(self.faults.fail_after) => Err(injected(what)),
            other => other,
        }
    }
}

impl fmt::Display for Faulty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Faulty({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.before("put").await?;
        let r = self.inner.put_opts(location, payload, opts).await;
        self.after("put response", r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.before("multipart").await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.before("get").await?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.before("get ranges").await?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Deletes apply; some responses are lost.
        let (rng, enabled, injected_count, p) = (
            self.rng.clone(),
            self.enabled.clone(),
            self.injected.clone(),
            self.faults.fail_after,
        );
        self.inner
            .delete_stream(locations)
            .map(move |r| {
                let roll = splitmix(rng.fetch_add(1, Ordering::Relaxed));
                let lost = enabled.load(Ordering::Relaxed)
                    && ((roll >> 11) as f64 / (1u64 << 53) as f64) < p;
                match r {
                    Ok(_) if lost => {
                        injected_count.fetch_add(1, Ordering::Relaxed);
                        Err(injected("delete response"))
                    }
                    other => other,
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.hit(self.faults.fail_before) {
            true => futures::stream::once(async { Err(injected("list")) }).boxed(),
            false => self.inner.list(prefix),
        }
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        match self.hit(self.faults.fail_before) {
            true => futures::stream::once(async { Err(injected("list")) }).boxed(),
            false => self.inner.list_with_offset(prefix, offset),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.before("list").await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.before("copy").await?;
        let r = self.inner.copy_opts(from, to, options).await;
        self.after("copy response", r)
    }
}
