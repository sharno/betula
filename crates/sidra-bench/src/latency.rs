//! Simulated object-store latency, added by the metered store to every
//! request.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// What `--latency` asks for.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LatencyArg {
    /// Requests take as long as the backing store takes.
    None,
    /// The same delay on every request.
    Fixed(Duration),
    /// Sampled per request from [`S3`].
    S3,
}

impl FromStr for LatencyArg {
    type Err = String;

    /// `none`, `s3`, or a fixed delay such as `20ms`.
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "none" => Ok(LatencyArg::None),
            "s3" => Ok(LatencyArg::S3),
            _ => s
                .strip_suffix("ms")
                .and_then(|n| n.parse().ok())
                .map(|ms| LatencyArg::Fixed(Duration::from_millis(ms)))
                .ok_or_else(|| format!("expected none, s3 or <n>ms, got {s:?}")),
        }
    }
}

impl fmt::Display for LatencyArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LatencyArg::None => write!(f, "none"),
            LatencyArg::Fixed(d) => write!(f, "{}ms", d.as_millis()),
            LatencyArg::S3 => write!(f, "s3"),
        }
    }
}

/// Request classes with different latency.
#[derive(Clone, Copy, Debug)]
pub enum Op {
    /// GET, HEAD, LIST.
    Read,
    /// PUT, multipart part, COPY.
    Write,
}

/// Latency of one request class.
#[derive(Clone, Copy, Debug)]
pub struct OpProfile {
    /// Time to first byte is log-normal with this median and p99.
    pub median: Duration,
    pub p99: Duration,
    /// This share of requests also stalls for `stall_min` plus up to `stall_spread`: slow
    /// replicas, dropped connections, retried 503s. These set p99.9 and up.
    pub stall_share: f64,
    pub stall_min: Duration,
    pub stall_spread: Duration,
    /// Per-request transfer rate, so large requests take longer.
    pub bytes_per_sec: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Profile {
    pub read: OpProfile,
    pub write: OpProfile,
}

/// S3 Standard from a client in the same region. Rough public figures, not
/// a calibrated fit: GET p50 ~20 ms, p99 ~120 ms; PUT p50 ~40 ms, p99
/// ~250 ms; about 1 in 1,000 requests stalls for up to seconds; ~80 MiB/s
/// per connection.
pub const S3: Profile = Profile {
    read: OpProfile {
        median: Duration::from_millis(20),
        p99: Duration::from_millis(120),
        stall_share: 0.001,
        stall_min: Duration::from_millis(500),
        stall_spread: Duration::from_millis(1500),
        bytes_per_sec: 80.0 * 1024.0 * 1024.0,
    },
    write: OpProfile {
        median: Duration::from_millis(40),
        p99: Duration::from_millis(250),
        stall_share: 0.001,
        stall_min: Duration::from_millis(500),
        stall_spread: Duration::from_millis(2500),
        bytes_per_sec: 80.0 * 1024.0 * 1024.0,
    },
};

/// Same seed for every run and engine, so the n-th request of a run always
/// draws the same delay.
const SEED: u64 = 0x5EED_5EED;

/// z-score of the 99th percentile of a standard normal.
const Z99: f64 = 2.326_347_874;

/// Latency added to each request.
#[derive(Debug)]
pub enum Latency {
    None,
    Fixed(Duration),
    Sampled { profile: Profile, next: AtomicU64 },
}

impl Latency {
    pub fn new(arg: LatencyArg) -> Self {
        match arg {
            LatencyArg::None => Latency::None,
            LatencyArg::Fixed(d) => Latency::Fixed(d),
            LatencyArg::S3 => Latency::Sampled {
                profile: S3,
                next: AtomicU64::new(0),
            },
        }
    }

    /// The delay for the next request of class `op` moving `bytes`.
    pub fn delay(&self, op: Op, bytes: u64) -> Duration {
        match self {
            Latency::None => Duration::ZERO,
            Latency::Fixed(d) => *d,
            Latency::Sampled { profile, next } => {
                let p = match op {
                    Op::Read => &profile.read,
                    Op::Write => &profile.write,
                };
                sample(p, next.fetch_add(1, Ordering::Relaxed), bytes)
            }
        }
    }
}

/// A uniform draw in (0, 1], the `k`-th of request `i`.
fn uniform(i: u64, k: u64) -> f64 {
    let x = crate::splitmix(SEED ^ i.wrapping_mul(4).wrapping_add(k));
    ((x >> 11) as f64 + 1.0) / (1u64 << 53) as f64
}

/// The delay of request `i`: a pure function of the profile, the request
/// index and its size.
pub fn sample(p: &OpProfile, i: u64, bytes: u64) -> Duration {
    // Box–Muller: a standard normal from two uniforms.
    let z = (-2.0 * uniform(i, 0).ln()).sqrt() * (std::f64::consts::TAU * uniform(i, 1)).cos();
    let sigma = (p.p99.as_secs_f64() / p.median.as_secs_f64()).ln() / Z99;
    let first_byte = p.median.as_secs_f64() * (sigma * z).exp();
    let stall = if uniform(i, 2) <= p.stall_share {
        p.stall_min.as_secs_f64() + p.stall_spread.as_secs_f64() * uniform(i, 3)
    } else {
        0.0
    };
    let transfer = bytes as f64 / p.bytes_per_sec;
    Duration::from_secs_f64(first_byte + stall + transfer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn percentiles(p: &OpProfile, bytes: u64) -> impl Fn(f64) -> f64 {
        let mut ms: Vec<f64> = (0..200_000)
            .map(|i| sample(p, i, bytes).as_secs_f64() * 1e3)
            .collect();
        ms.sort_by(f64::total_cmp);
        move |q| ms[((ms.len() as f64 * q) as usize).min(ms.len() - 1)]
    }

    #[test]
    fn s3_read_matches_profile() {
        let pct = percentiles(&S3.read, 0);
        assert!((pct(0.5) - 20.0).abs() < 1.0, "p50 {}", pct(0.5));
        assert!((pct(0.99) - 120.0).abs() < 10.0, "p99 {}", pct(0.99));
        assert!(pct(0.9995) > 500.0, "p99.95 {}", pct(0.9995));
    }

    #[test]
    fn s3_write_matches_profile() {
        let pct = percentiles(&S3.write, 0);
        assert!((pct(0.5) - 40.0).abs() < 2.0, "p50 {}", pct(0.5));
        assert!((pct(0.99) - 250.0).abs() < 20.0, "p99 {}", pct(0.99));
    }

    #[test]
    fn transfer_adds_size_over_rate() {
        let d = sample(&S3.read, 7, 8 << 20) - sample(&S3.read, 7, 0);
        assert!((d.as_secs_f64() - 0.1).abs() < 1e-6, "{d:?}");
    }

    #[test]
    fn parses_args() {
        assert_eq!("s3".parse(), Ok(LatencyArg::S3));
        assert_eq!("none".parse(), Ok(LatencyArg::None));
        assert_eq!(
            "20ms".parse(),
            Ok(LatencyArg::Fixed(Duration::from_millis(20)))
        );
        assert!("20".parse::<LatencyArg>().is_err());
    }
}
