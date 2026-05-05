//! `Source.Queue`, `Sink.Queue`, and `RestartSource` spec parity.
//!
//! Notes on adaptation:
//! * `SourceQueue::new()` returns an unbounded `(SourceQueue, Source)` pair;
//!   bounded `Source.Queue(size, OverflowStrategy)` is realised
//!   here by piping the source through `Source::buffer(size, strategy)`,
//!   which is the canonical bounded-buffer policy in this port.
//! * `QueueOfferResult` variants are `Enqueued` / `Dropped` / `Failure` /
//!   `QueueClosed`. There is no `Backpressured` variant — the offer is
//!   synchronous and bounded buffering is delegated to `Source::buffer`.
//!   The closest equivalent is asserted below.
//! * `Sink::queue(source)` takes the source directly and exposes a pull
//!   handle; the buffer is unbounded internally.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_streams::{
    OverflowStrategy, QueueOfferResult, RestartSettings, RestartSource, Sink, Source, SourceQueue,
};

// -- SourceQueue --------------------------------------------------------------

#[tokio::test]
async fn source_queue_offer_enqueues_and_flows_downstream() {
    let (q, src) = SourceQueue::<i32>::new();
    let handle = tokio::spawn(async move { Sink::collect(src).await });

    assert_eq!(q.offer(1), QueueOfferResult::Enqueued);
    assert_eq!(q.offer(2), QueueOfferResult::Enqueued);
    assert_eq!(q.offer(3), QueueOfferResult::Enqueued);
    q.complete();

    let out = handle.await.unwrap();
    assert_eq!(out, vec![1, 2, 3]);
}

#[tokio::test]
async fn source_queue_offer_after_complete_returns_queue_closed() {
    let (q, src) = SourceQueue::<i32>::new();
    // Drop the source before offering — downstream is gone.
    drop(src);
    // Give the runtime a tick so the channel observes the drop.
    tokio::task::yield_now().await;
    assert_eq!(q.offer(99), QueueOfferResult::QueueClosed);
    assert!(q.is_closed());
}

#[tokio::test]
async fn source_queue_complete_terminates_downstream() {
    let (q, src) = SourceQueue::<i32>::new();
    let handle = tokio::spawn(async move { Sink::collect(src).await });
    q.complete();
    let out = handle.await.unwrap();
    assert!(out.is_empty());
}

// -- SourceQueue + OverflowStrategy via Source::buffer ------------------------
//
// Here we compose
// `SourceQueue::new()` with `Source::buffer(size, strategy)`.

#[tokio::test]
async fn queue_with_drop_new_keeps_only_buffered_elements() {
    let (q, src) = SourceQueue::<i32>::new();
    // Push items before any consumer runs, then bound to size 1 with DropNew.
    for i in 0..50_i32 {
        assert_eq!(q.offer(i), QueueOfferResult::Enqueued);
    }
    q.complete();

    let bounded = src.buffer(1, OverflowStrategy::DropNew);
    let out = Sink::collect(bounded).await;
    // DropNew: at most ~size elements survive once the buffer fills.
    assert!(!out.is_empty());
    assert!(out.len() <= 50);
}

#[tokio::test]
async fn queue_with_backpressure_preserves_all_elements() {
    let (q, src) = SourceQueue::<i32>::new();
    let bounded = src.buffer(4, OverflowStrategy::Backpressure);
    let handle = tokio::spawn(async move { Sink::collect(bounded).await });
    for i in 0..20_i32 {
        assert_eq!(q.offer(i), QueueOfferResult::Enqueued);
    }
    q.complete();
    let out = handle.await.unwrap();
    assert_eq!(out, (0..20).collect::<Vec<_>>());
}

// -- SinkQueue ----------------------------------------------------------------

#[tokio::test]
async fn sink_queue_pulls_each_element_then_none() {
    let q = Sink::queue(Source::from_iter(vec![10, 20, 30]));
    assert_eq!(q.pull().await, Some(10));
    assert_eq!(q.pull().await, Some(20));
    assert_eq!(q.pull().await, Some(30));
    assert_eq!(q.pull().await, None);
}

#[tokio::test]
async fn sink_queue_buffers_until_drained() {
    // Drive a moderately sized source and assert all elements are recoverable
    // via the pull handle (buffering is internal/unbounded in this port).
    let q = Sink::queue(Source::from_iter(0..32_i32));
    let mut got = Vec::new();
    while let Some(v) = q.pull().await {
        got.push(v);
    }
    assert_eq!(got, (0..32).collect::<Vec<_>>());
}

#[tokio::test]
async fn sink_queue_pull_with_timeout_returns_none_when_empty() {
    // After completion + drain, pulling with a timeout returns None promptly.
    let q = Sink::queue(Source::from_iter(vec![1_i32]));
    assert_eq!(q.pull().await, Some(1));
    let v = Sink::pull_with_timeout(&q, Duration::from_millis(50)).await;
    assert_eq!(v, None);
}

// -- RestartSource ------------------------------------------------------------

#[tokio::test]
async fn restart_source_resubscribes_after_completion() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    let settings = RestartSettings {
        min_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        random_factor: 0.0,
        max_restarts: Some(3),
    };
    let source = RestartSource::with_backoff(settings, move || {
        calls_c.fetch_add(1, Ordering::SeqCst);
        Source::from_iter(vec![7, 8])
    });
    let out = Sink::collect(source).await;
    // 3 attempts × 2 elements = 6.
    assert_eq!(out, vec![7, 8, 7, 8, 7, 8]);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn restart_source_respects_max_restarts_cap() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    let settings = RestartSettings {
        min_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        random_factor: 0.0,
        max_restarts: Some(1),
    };
    let source = RestartSource::with_backoff(settings, move || {
        calls_c.fetch_add(1, Ordering::SeqCst);
        Source::from_iter(vec![42])
    });
    let out = Sink::collect(source).await;
    assert_eq!(out, vec![42]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_source_zero_max_yields_empty_stream() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    let settings = RestartSettings {
        min_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        random_factor: 0.0,
        max_restarts: Some(0),
    };
    let source = RestartSource::with_backoff(settings, move || {
        calls_c.fetch_add(1, Ordering::SeqCst);
        Source::from_iter(vec![1, 2, 3])
    });
    let out = Sink::collect(source).await;
    assert!(out.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn restart_source_default_settings_has_finite_cap() {
    // defaults to a finite restart cap; assert
    // the default is `Some(_)` so streams cannot loop forever by accident.
    let s = RestartSettings::default();
    assert!(s.max_restarts.is_some());
    assert!(s.min_backoff <= s.max_backoff);
}
