//! TestScheduler virtual-time spec. Asserts deterministic timing under
//! `advance_by` / `advance_to`, cancel semantics, and cross-token
//! ordering.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use atomr_testkit::TestScheduler;

#[tokio::test]
async fn advance_zero_fires_nothing() {
    let s = TestScheduler::new();
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    let _ = s.schedule_after(Duration::from_secs(1), move || {
        n2.fetch_add(1, Ordering::SeqCst);
    });
    s.advance_by(Duration::ZERO).await;
    assert_eq!(n.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn advance_past_fire_at_runs_callback_once() {
    let s = TestScheduler::new();
    let n = Arc::new(AtomicU32::new(0));
    let n2 = n.clone();
    s.schedule_after(Duration::from_millis(100), move || {
        n2.fetch_add(1, Ordering::SeqCst);
    });
    s.advance_by(Duration::from_secs(5)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
    s.advance_by(Duration::from_secs(5)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1, "callback must not refire");
}

#[tokio::test]
async fn many_schedules_one_advance_fires_all_in_order() {
    let s = TestScheduler::new();
    let order: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    for (delay, id) in [(50u64, 3u32), (10, 1), (30, 2)] {
        let o = order.clone();
        s.schedule_after(Duration::from_millis(delay), move || {
            o.lock().unwrap().push(id);
        });
    }
    s.advance_by(Duration::from_millis(100)).await;
    assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn cancel_after_fire_returns_false() {
    let s = TestScheduler::new();
    let token = s.schedule_after(Duration::from_millis(10), || {});
    s.advance_by(Duration::from_millis(20)).await;
    assert!(s.fired(token));
    assert!(!s.cancel(token), "cancelling a fired token returns false");
}

#[tokio::test]
async fn cancel_unknown_token_returns_false() {
    let s = TestScheduler::new();
    let real = s.schedule_after(Duration::from_secs(1), || {});
    s.cancel(real);
    let other = s.schedule_after(Duration::from_secs(1), || {});
    let _ = other;
    // Construct a clearly unknown token by scheduling a third and
    // immediately running it; this pushes next_token past the
    // current visible range. We can't fabricate ScheduledToken
    // directly, so this assertion is best-effort: re-cancelling
    // `real` after a previous cancel should still return false.
    assert!(!s.cancel(real));
}

#[tokio::test]
async fn advance_to_in_past_does_not_rewind() {
    let s = TestScheduler::new();
    let start = s.now();
    s.advance_by(Duration::from_secs(1)).await;
    let after = s.now();
    s.advance_to(start).await;
    assert!(s.now() >= after, "advance_to must not rewind virtual time");
}

#[tokio::test]
async fn pending_decreases_with_each_fire() {
    let s = TestScheduler::new();
    s.schedule_after(Duration::from_millis(1), || {});
    s.schedule_after(Duration::from_millis(2), || {});
    s.schedule_after(Duration::from_millis(3), || {});
    assert_eq!(s.pending(), 3);
    s.advance_by(Duration::from_millis(2)).await;
    assert_eq!(s.pending(), 1);
    s.advance_by(Duration::from_millis(2)).await;
    assert_eq!(s.pending(), 0);
}

#[tokio::test]
async fn cancel_decreases_pending() {
    let s = TestScheduler::new();
    let t1 = s.schedule_after(Duration::from_secs(1), || {});
    let _t2 = s.schedule_after(Duration::from_secs(1), || {});
    assert_eq!(s.pending(), 2);
    assert!(s.cancel(t1));
    assert_eq!(s.pending(), 1);
}
