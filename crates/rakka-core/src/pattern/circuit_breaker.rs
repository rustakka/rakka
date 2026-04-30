//! Circuit breaker. akka.net: `Pattern/CircuitBreaker.cs`.

use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
                if Instant::now().elapsed().as_nanos() as u64 >= self.reset_timeout.as_nanos() as u64 {
                    CircuitBreakerState::HalfOpen
                } else {
                    CircuitBreakerState::Open
                }
            }
            _ => CircuitBreakerState::HalfOpen,
        }
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
            self.opened_at_ns.store(Instant::now().elapsed().as_nanos() as u64, Ordering::Release);
        }
    }
}

#[derive(Debug, thiserror::Error)]
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
