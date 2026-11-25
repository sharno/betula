//! `ObjectStore` wrapper that injects faults: requests that fail before
//! reaching the store, requests whose response is lost after the store
//! applied them, and random delays that reorder concurrent requests.
//! Deterministic per seed (up to task scheduling).

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

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

impl Faults {
    pub const NONE: Faults = Faults {
        fail_before: 0.0,
        fail_after: 0.0,
        max_delay: Duration::ZERO,
    };
}

/// Holds PUTs to matching paths in flight until released, like a request
/// stuck in the network. Independent of `Faults`.
pub struct Hold {
    matches: Box<dyn Fn(&str) -> bool + Send + Sync>,
    /// How many PUTs to hold; later ones pass.
    max: usize,
    arrived: AtomicUsize,
    released: watch::Sender<bool>,
}

impl Hold {
    /// PUTs held so far.
    pub fn arrived(&self) -> usize {
        self.arrived.load(Ordering::SeqCst)
    }

    pub fn release(&self) {
        self.released.send_replace(true);
    }

    fn take(&self, path: &str) -> bool {
        (self.matches)(path)
            && !*self.released.borrow()
            && self
                .arrived
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    (n < self.max).then_some(n + 1)
                })
                .is_ok()
    }

    async fn wait(&self) {
        let mut rx = self.released.subscribe();
        let _ = rx.wait_for(|r| *r).await;
    }
}

impl fmt::Debug for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hold(arrived: {})", self.arrived())
    }
}

#[derive(Debug)]
pub struct Faulty {
    inner: Arc<dyn ObjectStore>,
    faults: Faults,
    rng: Arc<AtomicU64>,
    enabled: Arc<AtomicBool>,
    holds: Mutex<Vec<Arc<Hold>>>,
    pub injected: Arc<AtomicU64>,
    /// Requests seen and an order-sensitive digest of them, so a simulation
    /// can check that a seed replays the same requests.
    trace: Arc<Mutex<Trace>>,
}

/// See `Faulty::trace`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    pub requests: u64,
    pub digest: u64,
}

impl Trace {
    fn record(&mut self, what: &str, path: &str) {
        use std::hash::{DefaultHasher, Hash, Hasher};
        // `DefaultHasher::new` uses fixed keys: the same in every run.
        let mut h = DefaultHasher::new();
        (what, path).hash(&mut h);
        self.requests += 1;
        self.digest = splitmix(self.digest.rotate_left(5) ^ h.finish());
    }
}

pub fn splitmix(mut z: u64) -> u64 {
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
            holds: Mutex::new(Vec::new()),
            injected: Arc::default(),
            trace: Arc::default(),
        }
    }

    pub fn trace(&self) -> Trace {
        *self.trace.lock().unwrap()
    }

    fn record(&self, what: &str, path: &str) {
        self.trace.lock().unwrap().record(what, path);
    }

    /// Holds up to `max` PUTs whose path matches until the hold is released.
    pub fn hold_puts(
        &self,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
        max: usize,
    ) -> Arc<Hold> {
        let hold = Arc::new(Hold {
            matches: Box::new(matches),
            max,
            arrived: AtomicUsize::new(0),
            released: watch::channel(false).0,
        });
        self.holds.lock().unwrap().push(hold.clone());
        hold
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

    async fn before(&self, what: &str, path: &Path) -> Result<()> {
        self.record(what, path.as_ref());
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
        let hold = self
            .holds
            .lock()
            .unwrap()
            .iter()
            .find(|h| h.take(location.as_ref()))
            .cloned();
        if let Some(h) = hold {
            h.wait().await;
        }
        self.before("put", location).await?;
        let r = self.inner.put_opts(location, payload, opts).await;
        self.after("put response", r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.before("multipart", location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.before("get", location).await?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.before("get ranges", location).await?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.record("delete", "");
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
        self.record("list", prefix.map_or("", |p| p.as_ref()));
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
        self.record("list from", offset.as_ref());
        match self.hit(self.faults.fail_before) {
            true => futures::stream::once(async { Err(injected("list")) }).boxed(),
            false => self.inner.list_with_offset(prefix, offset),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.before("list", prefix.unwrap_or(&Path::default()))
            .await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.before("copy", from).await?;
        let r = self.inner.copy_opts(from, to, options).await;
        self.after("copy response", r)
    }
}
