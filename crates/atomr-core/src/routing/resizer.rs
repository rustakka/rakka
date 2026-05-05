//! Pool resizer config. akka.net: `Routing/Resizer.cs`,
//! `DefaultResizer.cs`, `ResizerSpec`.
//!
//! A resizer monitors pressure on a pool of routees and decides whether
//! to grow or shrink it. atomr's pool routers (RoundRobin, Random,
//! SmallestMailbox, …) accept a [`ResizerConfig`] and implement
//! [`ResizerConfig::compute_delta`] to advise the parent on how many
//! routees to add or remove.
//!
//! The semantics mirror akka.net's `DefaultResizer`:
//!   * `lower_bound` ≤ pool size ≤ `upper_bound`
//!   * pressure is measured as the count of busy routees
//!   * if pressure ≥ `pressure_threshold * pool_size` for
//!     `messages_per_resize` messages, grow by `rampup_rate * pool_size`
//!     (rounded up, clamped to upper bound)
//!   * if pressure ≤ `backoff_threshold * pool_size` after a delay,
//!     shrink by `backoff_rate * pool_size`

use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct ResizerConfig {
    pub lower_bound: usize,
    pub upper_bound: usize,
    /// Fraction of busy routees that triggers growth (0.0..=1.0).
    pub pressure_threshold: f64,
    /// Fraction of routees needed to be idle to trigger backoff.
    pub backoff_threshold: f64,
    /// Multiplicative ramp-up factor applied to the current size.
    pub rampup_rate: f64,
    /// Multiplicative ramp-down factor applied to the current size.
    pub backoff_rate: f64,
    /// How many messages must be processed before checking pressure
    /// again.
    pub messages_per_resize: u64,
    /// Idle interval before considering a backoff resize.
    pub backoff_delay: Duration,
}

impl Default for ResizerConfig {
    fn default() -> Self {
        Self {
            lower_bound: 1,
            upper_bound: 10,
            pressure_threshold: 1.0,
            backoff_threshold: 0.3,
            rampup_rate: 0.2,
            backoff_rate: 0.1,
            messages_per_resize: 10,
            backoff_delay: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResizeAdvice {
    /// Net change to apply: positive grows the pool, negative shrinks
    /// it, zero leaves it alone.
    pub delta: i32,
}

impl ResizerConfig {
    /// Decide how many routees to add or remove given the current pool
    /// size and the count of currently-busy routees. Returns the net
    /// delta clamped to `[lower_bound, upper_bound]`.
    pub fn compute_delta(&self, current_size: usize, busy: usize) -> ResizeAdvice {
        if current_size == 0 {
            // Always grow to at least the lower bound.
            return ResizeAdvice { delta: self.lower_bound as i32 };
        }
        let load = busy as f64 / current_size as f64;
        let target = if load >= self.pressure_threshold {
            let grown = current_size as f64 * (1.0 + self.rampup_rate);
            grown.ceil() as usize
        } else if load <= self.backoff_threshold {
            let shrunk = current_size as f64 * (1.0 - self.backoff_rate);
            shrunk.floor() as usize
        } else {
            current_size
        };
        let clamped = target.clamp(self.lower_bound, self.upper_bound);
        ResizeAdvice { delta: clamped as i32 - current_size as i32 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_grows_under_pressure() {
        let r = ResizerConfig::default();
        let advice = r.compute_delta(2, 2);
        assert!(advice.delta > 0, "expected growth, got {:?}", advice);
    }

    #[test]
    fn default_shrinks_when_idle() {
        let r =
            ResizerConfig { lower_bound: 1, upper_bound: 10, backoff_rate: 0.5, ..Default::default() };
        let advice = r.compute_delta(8, 0);
        assert!(advice.delta < 0, "expected shrink, got {:?}", advice);
    }

    #[test]
    fn clamps_to_upper_bound() {
        let r = ResizerConfig {
            lower_bound: 1,
            upper_bound: 4,
            rampup_rate: 5.0,
            pressure_threshold: 0.5,
            ..Default::default()
        };
        let advice = r.compute_delta(3, 3);
        // Cannot exceed upper_bound (=4) regardless of rampup.
        assert_eq!(advice.delta, 1);
    }

    #[test]
    fn clamps_to_lower_bound() {
        let r = ResizerConfig {
            lower_bound: 2,
            upper_bound: 10,
            backoff_rate: 0.9,
            backoff_threshold: 0.5,
            ..Default::default()
        };
        let advice = r.compute_delta(3, 0);
        assert_eq!(advice.delta, -1);
    }

    #[test]
    fn zero_size_grows_to_lower_bound() {
        let r = ResizerConfig { lower_bound: 3, ..Default::default() };
        let advice = r.compute_delta(0, 0);
        assert_eq!(advice.delta, 3);
    }

    #[test]
    fn no_change_when_load_in_band() {
        let r = ResizerConfig {
            pressure_threshold: 0.9,
            backoff_threshold: 0.1,
            ..Default::default()
        };
        let advice = r.compute_delta(5, 3);
        assert_eq!(advice.delta, 0);
    }
}
