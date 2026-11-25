//! Hybrid logical clock. Timestamps order writes: physical time while the
//! clock runs ahead of it, and a logical counter otherwise. A new writer
//! moves its clock past every timestamp it finds (`observe`), so writer
//! generations never reuse or undercut one.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Physical milliseconds in the high 48 bits and a logical counter in the
/// low 16. Unique: the one writer never hands out the same value twice.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ts(pub u64);

impl Ts {
    pub const MIN: Ts = Ts(0);
}

impl fmt::Display for Ts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) const LOGICAL_BITS: u32 = 16;

/// The timestamp at physical time `ms`, below any taken in that millisecond.
pub fn at_ms(ms: u64) -> Ts {
    Ts(ms << LOGICAL_BITS)
}

/// A duration in timestamp units.
pub fn span(d: std::time::Duration) -> u64 {
    at_ms(u64::try_from(d.as_millis()).unwrap_or(u64::MAX >> LOGICAL_BITS)).0
}

#[derive(Debug, Default)]
pub struct Hlc {
    last: AtomicU64,
}

impl Hlc {
    /// A timestamp greater than every one handed out or observed before.
    pub fn now(&self, physical_ms: u64) -> Ts {
        let physical = at_ms(physical_ms).0;
        let prev = self
            .last
            .try_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                Some(physical.max(last + 1))
            })
            .expect("the update closure always returns Some");
        Ts(physical.max(prev + 1))
    }

    /// Moves the clock past a timestamp found in stored data.
    pub fn observe(&self, ts: Ts) {
        self.last.fetch_max(ts.0, Ordering::AcqRel);
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
        c.observe(Ts(d.0 + 100));
        assert!(c.now(1_000).0 > d.0 + 100);
    }
}
