//! Hybrid logical clock. Timestamps order writes across writers: physical
//! time when clocks agree, and causal order when a writer has seen another
//! writer's data (`observe`).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Random per process. Never reused, so writers need no coordination.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WriterId(pub u64);

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl WriterId {
    pub fn parse(s: &str) -> Option<Self> {
        u64::from_str_radix(s, 16).ok().map(Self)
    }
}

/// `hlc` packs physical milliseconds in the high 48 bits and a logical counter
/// in the low 16. The writer id breaks ties, so timestamps are unique.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ts {
    pub hlc: u64,
    pub writer: WriterId,
}

impl Ts {
    pub const MIN: Ts = Ts {
        hlc: 0,
        writer: WriterId(0),
    };
}

const LOGICAL_BITS: u32 = 16;

#[derive(Debug, Default)]
pub struct Hlc {
    last: AtomicU64,
}

impl Hlc {
    /// A timestamp greater than every one handed out or observed before.
    pub fn now(&self, physical_ms: u64) -> u64 {
        let physical = physical_ms << LOGICAL_BITS;
        let prev = self
            .last
            .try_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                Some(physical.max(last + 1))
            })
            .expect("the update closure always returns Some");
        physical.max(prev + 1)
    }

    /// Moves the clock past a timestamp seen in data from another writer.
    pub fn observe(&self, hlc: u64) {
        self.last.fetch_max(hlc, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_even_if_physical_time_goes_back() {
        let c = Hlc::default();
        let a = c.now(1_000);
        let b = c.now(999);
        let d = c.now(1_000);
        assert!(a < b && b < d);
        c.observe(d + 100);
        assert!(c.now(1_000) > d + 100);
    }
}
