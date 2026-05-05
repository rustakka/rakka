//! Circuit breaker stress + state-transition spec parity.
//! `CircuitBreakerSpec`, `CircuitBreakerStressSpec`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_core::pattern::{CircuitBreaker, CircuitBreakerError, CircuitBreakerState};

#[tokio::test]
async fn closed_state_passes_through_results() {
    let cb = CircuitBreaker::new(3, Duration::from_secs(1), Duration::from_secs(1));
    let r: Result<u32, CircuitBreakerError<()>> = cb.call(|| async { Ok(42) }).await;
    assert_eq!(r.unwrap(), 42);
    assert_eq!(cb.state(), CircuitBreakerState::Closed);
}

#[tokio::test]
async fn opens_after_max_failures_and_rejects_calls() {
    let cb = CircuitBreaker::new(2, Duration::from_secs(1), Duration::from_secs(1));
    for _ in 0..2 {
        let _: Result<(), _> = cb.call(|| async { Err::<(), u32>(1) }).await;
    }
    assert_eq!(cb.state(), CircuitBreakerState::Open);
    let r: Result<(), _> = cb.call(|| async { Ok::<(), u32>(()) }).await;
    assert!(matches!(r, Err(CircuitBreakerError::Open)));
}

#[tokio::test]
async fn timeout_counts_as_a_failure() {
    let cb = CircuitBreaker::new(1, Duration::from_millis(20), Duration::from_secs(1));
    let r: Result<(), CircuitBreakerError<()>> = cb
        .call(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        })
        .await;
    assert!(matches!(r, Err(CircuitBreakerError::Timeout)));
    assert_eq!(cb.state(), CircuitBreakerState::Open);
}

#[tokio::test]
async fn success_resets_failure_counter() {
    let cb = CircuitBreaker::new(3, Duration::from_secs(1), Duration::from_secs(1));
    let _: Result<(), _> = cb.call(|| async { Err::<(), u32>(1) }).await;
    let _: Result<(), _> = cb.call(|| async { Err::<(), u32>(2) }).await;
    // Two failures of three — still closed.
    assert_eq!(cb.state(), CircuitBreakerState::Closed);
    let _: Result<u32, CircuitBreakerError<u32>> = cb.call(|| async { Ok(7) }).await;
    // After success, the counter is reset; another failure shouldn't open.
    let _: Result<(), _> = cb.call(|| async { Err::<(), u32>(3) }).await;
    assert_eq!(cb.state(), CircuitBreakerState::Closed);
}

#[tokio::test]
async fn open_transitions_to_half_open_after_reset_timeout() {
    let cb = CircuitBreaker::new(1, Duration::from_secs(1), Duration::from_millis(40));
    let _: Result<(), _> = cb.call(|| async { Err::<(), u32>(1) }).await;
    assert_eq!(cb.state(), CircuitBreakerState::Open);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(cb.state(), CircuitBreakerState::HalfOpen);
}

#[tokio::test]
async fn stress_concurrent_calls_eventually_open_when_failing() {
    let cb = CircuitBreaker::new(20, Duration::from_secs(1), Duration::from_secs(10));
    let success = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::new();
    for _ in 0..50 {
        let cb = cb.clone();
        let s = success.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..3 {
                let r: Result<(), CircuitBreakerError<u32>> = cb.call(|| async { Err::<(), u32>(1) }).await;
                if matches!(r, Err(CircuitBreakerError::Inner(_))) {
                    s.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(cb.state(), CircuitBreakerState::Open);
    // At least the first 20 attempts succeed in invoking the inner function;
    // subsequent ones see Open.
    assert!(success.load(Ordering::SeqCst) >= 20);
}
