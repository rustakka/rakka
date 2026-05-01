//! Circuit breaker. akka.net: `Pattern/CircuitBreaker.cs`.

use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    max_failures: u32,
    call_timeout: Duration,
    reset_timeout: Duration,
    failures: AtomicU32,
    opened_at_ns: AtomicU64,
    // packed state: 0=closed, 1=open, 2=half-open
    state: AtomicU32,
}

impl CircuitBreaker {
    pub fn new(max_failures: u32, call_timeout: Duration, reset_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            max_failures,
            call_timeout,
            reset_timeout,
            failures: AtomicU32::new(0),
            opened_at_ns: AtomicU64::new(0),
            state: AtomicU32::new(0),
        })
    }

    pub fn state(&self) -> CircuitBreakerState {
        match self.state.load(Ordering::Acquire) {
            0 => CircuitBreakerState::Closed,
            1 => {
                // Compare elapsed since the breaker opened (epoch in
                // ns since process start) — Phase 3.4 fix; the
                // previous comparison used `Instant::now().elapsed()`
                // which is always 0 and never transitioned to half-open.
                let now_ns = self.elapsed_ns();
                let opened_ns = self.opened_at_ns.load(Ordering::Acquire);
                if opened_ns > 0
                    && now_ns.saturating_sub(opened_ns)
                        >= self.reset_timeout.as_nanos() as u64
                {
                    CircuitBreakerState::HalfOpen
                } else {
                    CircuitBreakerState::Open
                }
            }
            _ => CircuitBreakerState::HalfOpen,
        }
    }

    fn elapsed_ns(&self) -> u64 {
        // Stable epoch chosen at first call of `record_failure`. We
        // approximate via `std::time::SystemTime` + `UNIX_EPOCH` so
        // both `record_failure` and `state()` agree.
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let st = self.state.load(Ordering::Acquire);
        if st == 1 {
            return Err(CircuitBreakerError::Open);
        }
        let res = tokio::time::timeout(self.call_timeout, f()).await;
        match res {
            Ok(Ok(v)) => {
                self.failures.store(0, Ordering::Release);
                self.state.store(0, Ordering::Release);
                Ok(v)
            }
            Ok(Err(e)) => {
                self.record_failure();
                Err(CircuitBreakerError::Inner(e))
            }
            Err(_) => {
                self.record_failure();
                Err(CircuitBreakerError::Timeout)
            }
        }
    }

    fn record_failure(&self) {
        let n = self.failures.fetch_add(1, Ordering::AcqRel) + 1;
        if n >= self.max_failures {
            self.state.store(1, Ordering::Release);
            self.opened_at_ns
                .store(self.elapsed_ns(), Ordering::Release);
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CircuitBreakerError<E> {
    #[error("circuit breaker is open")]
    Open,
    #[error("call timed out")]
    Timeout,
    #[error(transparent)]
    Inner(E),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opens_after_max_failures() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(1), Duration::from_secs(1));
        for _ in 0..2 {
            let _ = cb.call(|| async { Err::<(), _>(1) }).await;
        }
        let res: Result<(), _> = cb.call(|| async { Ok::<(), u32>(()) }).await;
        assert!(matches!(res, Err(CircuitBreakerError::Open)));
    }
}
