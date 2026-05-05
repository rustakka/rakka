//! Monotonic / system clock abstractions.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Monotonic, non-decreasing clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct MonotonicClock;

impl MonotonicClock {
    pub fn now(&self) -> Instant {
        Instant::now()
    }

    pub fn elapsed(&self, since: Instant) -> Duration {
        self.now().duration_since(since)
    }
}

/// Wall-clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl SystemClock {
    pub fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    pub fn millis_since_epoch(&self) -> u64 {
        self.now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_non_decreasing() {
        let c = MonotonicClock;
        let a = c.now();
        let b = c.now();
        assert!(b >= a);
    }

    #[test]
    fn system_clock_returns_epoch_millis() {
        let c = SystemClock;
        assert!(c.millis_since_epoch() > 0);
    }
}
