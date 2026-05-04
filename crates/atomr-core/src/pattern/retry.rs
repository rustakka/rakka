//! `retry` — wrap an async fallible operation in a bounded retry loop
//! with optional fixed or exponential backoff.
//!
//! Phase 3.4 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Pattern.Retry` (with the same semantics as the JVM
//! `Patterns.retry`).
//!
//! ```ignore
//! use std::time::Duration;
//! use atomr_core::pattern::{retry, RetrySchedule};
//!
//! let result = retry(
//!     || async { fetch().await },
//!     5,
//!     RetrySchedule::exponential(Duration::from_millis(50), Duration::from_secs(2)),
//! ).await;
//! ```

use std::future::Future;
use std::time::Duration;

/// Schedule for the delay between attempts.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RetrySchedule {
    /// Fixed delay between every attempt.
    Fixed(Duration),
    /// Exponential backoff: `min`, `min*2`, `min*4`, … capped at `max`.
    Exponential { min: Duration, max: Duration },
}

impl RetrySchedule {
    pub fn fixed(d: Duration) -> Self {
        Self::Fixed(d)
    }

    pub fn exponential(min: Duration, max: Duration) -> Self {
        Self::Exponential { min, max }
    }

    /// Delay before the `attempt`th retry (0-indexed: attempt 0 is the
    /// first retry, i.e. after the initial call has already failed).
    pub fn delay_for(self, attempt: u32) -> Duration {
        match self {
            Self::Fixed(d) => d,
            Self::Exponential { min, max } => {
                let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
                let nanos = (min.as_nanos() as u64).saturating_mul(factor);
                let capped = nanos.min(max.as_nanos() as u64);
                Duration::from_nanos(capped)
            }
        }
    }
}

/// Run `op`, retrying up to `max_attempts` total times (including the
/// initial call). Returns the last error if every attempt fails.
///
/// `max_attempts == 1` means "no retries" — `op` runs exactly once.
pub async fn retry<T, E, F, Fut>(mut op: F, max_attempts: u32, schedule: RetrySchedule) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    assert!(max_attempts >= 1, "max_attempts must be ≥ 1");
    let mut last_err: Option<E> = None;
    for attempt in 0..max_attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < max_attempts {
                    tokio::time::sleep(schedule.delay_for(attempt)).await;
                }
            }
        }
    }
    Err(last_err.expect("loop ran ≥1 time"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn returns_immediately_on_first_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let c2 = calls.clone();
        let r: Result<i32, &'static str> = retry(
            move || {
                let c2 = c2.clone();
                async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }
            },
            5,
            RetrySchedule::fixed(Duration::from_millis(0)),
        )
        .await;
        assert_eq!(r, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_until_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let c2 = calls.clone();
        let r: Result<i32, &'static str> = retry(
            move || {
                let c2 = c2.clone();
                async move {
                    let n = c2.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err("not yet")
                    } else {
                        Ok(n as i32)
                    }
                }
            },
            5,
            RetrySchedule::fixed(Duration::from_millis(0)),
        )
        .await;
        assert_eq!(r, Ok(3));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn returns_last_error_after_max_attempts() {
        let r: Result<i32, &'static str> =
            retry(|| async { Err("nope") }, 3, RetrySchedule::fixed(Duration::from_millis(0))).await;
        assert_eq!(r, Err("nope"));
    }

    #[test]
    fn exponential_backoff_doubles_until_cap() {
        let s = RetrySchedule::exponential(Duration::from_millis(10), Duration::from_millis(80));
        assert_eq!(s.delay_for(0), Duration::from_millis(10));
        assert_eq!(s.delay_for(1), Duration::from_millis(20));
        assert_eq!(s.delay_for(2), Duration::from_millis(40));
        assert_eq!(s.delay_for(3), Duration::from_millis(80));
        assert_eq!(s.delay_for(10), Duration::from_millis(80)); // capped
    }

    #[test]
    #[should_panic]
    fn zero_max_attempts_panics() {
        let _ = futures::executor::block_on(retry::<(), &'static str, _, _>(
            || async { Ok(()) },
            0,
            RetrySchedule::fixed(Duration::ZERO),
        ));
    }
}
