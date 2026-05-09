//! Exponential-backoff reconnect policy for [`SerialTransport`].
//!
//! USB devices re-enumerate on cable wiggles, gadget reboots, and host
//! suspend/resume — much more often than a TCP socket gets RST'd. We
//! handle that inside the transport so [`atomr_remote::endpoint_manager`]
//! doesn't churn through `Pending → Quarantined` cycles on every flap.

use std::time::Duration;

/// How aggressively the transport retries a failed `open()` of the
/// configured device path.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// First retry after this delay.
    pub initial: Duration,
    /// Cap on the per-retry delay.
    pub max: Duration,
    /// Multiplier applied between retries.
    pub multiplier: f64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self { initial: Duration::from_millis(50), max: Duration::from_secs(5), multiplier: 2.0 }
    }
}

impl ReconnectPolicy {
    /// Disable reconnect entirely. The transport will surface
    /// `TransportError::Closed` on disconnect and never retry.
    pub fn never() -> Self {
        Self { initial: Duration::ZERO, max: Duration::ZERO, multiplier: 1.0 }
    }

    pub(crate) fn next_delay(&self, current: Duration) -> Duration {
        if self.max.is_zero() {
            return Duration::ZERO;
        }
        let scaled = current.mul_f64(self.multiplier);
        scaled.min(self.max)
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !self.max.is_zero()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_grows_to_cap_then_holds() {
        let p = ReconnectPolicy::default();
        let mut d = p.initial;
        for _ in 0..10 {
            d = p.next_delay(d);
        }
        assert_eq!(d, p.max, "exponential backoff should saturate at `max`");
    }

    #[test]
    fn never_disables_reconnect() {
        let p = ReconnectPolicy::never();
        assert!(!p.is_enabled());
        assert_eq!(p.next_delay(Duration::from_secs(1)), Duration::ZERO);
    }

    #[test]
    fn next_delay_uses_multiplier() {
        let p = ReconnectPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(10),
            multiplier: 3.0,
        };
        assert_eq!(p.next_delay(Duration::from_millis(100)), Duration::from_millis(300));
        assert_eq!(p.next_delay(Duration::from_millis(300)), Duration::from_millis(900));
    }
}
