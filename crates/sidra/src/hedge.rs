//! Hedged reads ("The Tail at Scale", Dean & Barroso): a GET still running
//! after the recent p95 latency gets a second copy, and the first answer
//! wins. An object store's slow tail comes from one slow server, connection
//! or retry, so the second copy rarely hits the same stall.

use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::{Either, select};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// Whether GETs are hedged.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Hedge {
    Off,
    On(HedgeConfig),
}

impl Default for Hedge {
    fn default() -> Self {
        Hedge::On(HedgeConfig::default())
    }
}

/// A number in `0..=1`, checked when parsed.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Fraction(f64);

impl Fraction {
    pub fn new(x: f64) -> Option<Self> {
        (0.0..=1.0).contains(&x).then_some(Self(x))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Fraction {
    type Error = String;
    fn try_from(x: f64) -> Result<Self, String> {
        Self::new(x).ok_or_else(|| format!("{x} is not in 0..=1"))
    }
}

impl From<Fraction> for f64 {
    fn from(f: Fraction) -> f64 {
        f.0
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct HedgeConfig {
    /// Latency percentile of recent GETs after which a second copy goes out.
    pub percentile: Fraction,
    /// Ceiling on second copies, as a share of GETs. When the store slows
    /// down as a whole, every GET passes the old threshold until it adapts;
    /// this keeps that from doubling the load.
    pub max_extra: Fraction,
    /// Number of recent GETs the percentile is taken over.
    pub window: NonZeroUsize,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            percentile: Fraction(0.95),
            max_extra: Fraction(0.1),
            window: DEFAULT_WINDOW,
        }
    }
}

// Checked at compile time.
const DEFAULT_WINDOW: NonZeroUsize = NonZeroUsize::new(512).unwrap();

/// GETs measured before the first threshold.
const MIN_SAMPLES: usize = 64;
/// GETs between threshold updates.
const UPDATE_EVERY: usize = 32;
/// Second copies that may go out back to back once the budget has built up.
const BURST: f64 = 10.0;

/// Hedges one store's GETs. Each store gets its own: stores in different
/// regions have different latency.
#[derive(Debug)]
pub enum Hedger {
    Off,
    On {
        config: HedgeConfig,
        tracker: Mutex<Tracker>,
    },
}

#[derive(Debug)]
enum Threshold {
    Learning,
    Ready(Duration),
}

#[derive(Debug)]
pub struct Tracker {
    /// The latest `window` GET latencies, oldest first.
    recent: VecDeque<Duration>,
    since_update: usize,
    threshold: Threshold,
    /// Second copies allowed now; each GET earns `max_extra`.
    budget: f64,
}

impl Hedger {
    pub fn new(hedge: Hedge) -> Self {
        match hedge {
            Hedge::Off => Hedger::Off,
            Hedge::On(config) => Hedger::On {
                config,
                tracker: Mutex::new(Tracker {
                    recent: VecDeque::with_capacity(config.window.get()),
                    since_update: 0,
                    threshold: Threshold::Learning,
                    budget: 0.0,
                }),
            },
        }
    }

    /// A hedger with the same settings and no history, for another store.
    pub fn fresh(&self) -> Self {
        Hedger::new(match self {
            Hedger::Off => Hedge::Off,
            Hedger::On { config, .. } => Hedge::On(*config),
        })
    }

    /// Runs `op`, and once more if the first run outlasts the threshold;
    /// returns whichever succeeds first. `hedges` counts second copies.
    pub async fn run<T, E, F, Fut>(&self, hedges: &AtomicU64, op: F) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let Hedger::On { config, tracker } = self else {
            return op().await;
        };
        let start = Instant::now();
        let first = pin!(op());
        let wait = {
            let mut t = tracker.lock();
            t.budget = (t.budget + config.max_extra.get()).min(BURST);
            match t.threshold {
                Threshold::Learning => None,
                Threshold::Ready(d) => Some(d),
            }
        };
        let record = |d: Duration| tracker.lock().record(d, config);
        let Some(wait) = wait else {
            let r = first.await;
            record(start.elapsed());
            return r;
        };
        let first = match select(first, pin!(tokio::time::sleep(wait))).await {
            Either::Left((r, _)) => {
                record(start.elapsed());
                return r;
            }
            Either::Right(((), first)) => first,
        };
        if !tracker.lock().spend() {
            let r = first.await;
            record(start.elapsed());
            return r;
        }
        hedges.fetch_add(1, Ordering::Relaxed);
        let second_start = Instant::now();
        let second = pin!(op());
        let r = match select(first, second).await {
            Either::Left((Ok(v), _)) => Ok(v),
            Either::Right((Ok(v), _)) => {
                record(second_start.elapsed());
                Ok(v)
            }
            // One copy failed: the other is the answer, failed or not.
            Either::Left((Err(_), second)) => second.await,
            Either::Right((Err(_), first)) => first.await,
        };
        // The first copy took at least this long. Recording it even when the
        // second won keeps slow GETs in the window, so the threshold doesn't
        // drift down as hedging hides the tail.
        record(start.elapsed());
        r
    }
}

impl Tracker {
    fn spend(&mut self) -> bool {
        let ok = self.budget >= 1.0;
        if ok {
            self.budget -= 1.0;
        }
        ok
    }

    fn record(&mut self, d: Duration, config: &HedgeConfig) {
        if self.recent.len() == config.window.get() {
            self.recent.pop_front();
        }
        self.recent.push_back(d);
        self.since_update += 1;
        if self.recent.len() >= MIN_SAMPLES && self.since_update >= UPDATE_EVERY {
            self.since_update = 0;
            if let Some(p) = percentile(&self.recent, config.percentile) {
                self.threshold = Threshold::Ready(p);
            }
        }
    }
}

/// The `q` quantile of `samples` (nearest rank); `None` when empty.
fn percentile(samples: &VecDeque<Duration>, q: Fraction) -> Option<Duration> {
    let mut sorted: Vec<_> = samples.iter().copied().collect();
    sorted.sort_unstable();
    let rank = (q.get() * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.saturating_sub(1)).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn percentile_is_nearest_rank() {
        let s: VecDeque<_> = (1..=100).map(ms).collect();
        let q = |x| Fraction::new(x).unwrap();
        assert_eq!(percentile(&s, q(0.95)), Some(ms(95)));
        assert_eq!(percentile(&s, q(1.0)), Some(ms(100)));
        assert_eq!(percentile(&VecDeque::new(), q(0.95)), None);
        assert_eq!(Fraction::new(1.5), None);
    }

    /// GETs that take 10 ms, except every `slow_every`-th first copy, which
    /// takes 5 s. Returns (calls made, hedges, total time).
    async fn run_gets(hedger: &Hedger, n: usize, slow_every: usize) -> (usize, u64, Duration) {
        let calls = AtomicUsize::new(0);
        let hedges = AtomicU64::new(0);
        let start = Instant::now();
        for i in 0..n {
            let first_call = calls.load(Ordering::Relaxed);
            let r: Result<usize, ()> = hedger
                .run(&hedges, || {
                    let call = calls.fetch_add(1, Ordering::Relaxed);
                    let slow = call == first_call && i % slow_every == slow_every - 1;
                    async move {
                        tokio::time::sleep(if slow { ms(5000) } else { ms(10) }).await;
                        Ok(i)
                    }
                })
                .await;
            assert_eq!(r, Ok(i));
        }
        (
            calls.load(Ordering::Relaxed),
            hedges.load(Ordering::Relaxed),
            start.elapsed(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn slow_gets_are_hedged_after_learning() {
        let hedger = Hedger::new(Hedge::default());
        let (calls, hedges, took) = run_gets(&hedger, 1000, 100).await;
        // The threshold is set after 64 GETs; the slow ones at 99, 199, ...
        // are all hedged at 10 ms, so none waits its 5 s.
        assert_eq!((calls, hedges), (1010, 10));
        assert_eq!(took, ms(990 * 10 + 10 * 20));
    }

    #[tokio::test(start_paused = true)]
    async fn off_never_hedges() {
        let (calls, hedges, took) = run_gets(&Hedger::new(Hedge::Off), 300, 100).await;
        assert_eq!((calls, hedges), (300, 0));
        assert!(took >= ms(3 * 5000));
    }

    #[tokio::test(start_paused = true)]
    async fn budget_caps_hedges_when_everything_is_slow() {
        let hedger = Hedger::new(Hedge::default());
        // Every first copy is slow once the threshold is set at 10 ms.
        let (_, _, _) = run_gets(&hedger, 100, usize::MAX).await;
        let (calls, hedges, _) = run_gets(&hedger, 200, 1).await;
        assert!(hedges <= (200.0 * 0.1 + BURST) as u64, "{hedges}");
        assert_eq!(calls as u64, 200 + hedges);
    }
}
