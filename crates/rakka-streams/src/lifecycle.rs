//! Lifecycle operators on `Source<T>`.
//!
//! Phase 12.8 of `docs/full-port-plan.md`. Akka.NET / Akka Streams
//! parity: `WatchTermination`, `Monitor`, `Log`. Each one wraps a
//! source and surfaces side-channel signals (completion, every
//! element, log line) without altering the elements themselves.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::stream::StreamExt;
use tokio::sync::oneshot;

use crate::source::Source;

/// `watch_termination(src)` returns the original source plus a
/// `oneshot::Receiver<()>` that fires when upstream completes
/// (whether by exhaustion or by the receiver being polled past the
/// final element).
///
/// Akka.NET: `Source.WatchTermination`.
pub fn watch_termination<T: Send + 'static>(src: Source<T>) -> (Source<T>, oneshot::Receiver<()>) {
    let (tx, rx) = oneshot::channel();
    let inner = src.into_boxed();
    let mut tx_holder = Some(tx);
    // `chain` a single synthetic element through a `filter_map` that
    // (a) drops the synthetic element so downstream sees only real
    // ones, and (b) fires the `tx` exactly once.
    let terminator = futures::stream::once(async {}).filter_map(move |()| {
        if let Some(tx) = tx_holder.take() {
            let _ = tx.send(());
        }
        std::future::ready(None::<T>)
    });
    let stream = inner.chain(terminator).boxed();
    (Source { inner: stream }, rx)
}

/// `monitor(src, on_each)` — invoke `on_each(&item)` for every
/// element flowing through, without consuming or transforming it.
/// Useful for telemetry instrumentation.
///
/// Akka.NET: `Source.Monitor`.
pub fn monitor<T, F>(src: Source<T>, mut on_each: F) -> Source<T>
where
    T: Send + 'static,
    F: FnMut(&T) + Send + 'static,
{
    let inner = src.into_boxed();
    Source { inner: inner.inspect(move |item| on_each(item)).boxed() }
}

/// `count_elements(src)` — convenience: returns the source unchanged
/// plus an `Arc<AtomicU64>` that totals every element.
///
/// Akka.NET: typically expressed as `monitor(... |_| counter.inc())`.
pub fn count_elements<T: Send + 'static>(src: Source<T>) -> (Source<T>, Arc<AtomicU64>) {
    let counter = Arc::new(AtomicU64::new(0));
    let c2 = counter.clone();
    (
        monitor(src, move |_| {
            c2.fetch_add(1, Ordering::Relaxed);
        }),
        counter,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::time::Duration;

    #[tokio::test]
    async fn watch_termination_fires_when_source_exhausts() {
        let s = Source::from_iter(vec![1, 2, 3]);
        let (src, term) = watch_termination(s);
        let collected = Sink::collect(src).await;
        assert_eq!(collected, vec![1, 2, 3]);
        tokio::time::timeout(Duration::from_millis(100), term)
            .await
            .expect("termination signal not received")
            .unwrap();
    }

    #[tokio::test]
    async fn monitor_observes_every_element() {
        let s = Source::from_iter(vec![10, 20, 30]);
        let observed = Arc::new(parking_lot::Mutex::new(Vec::<i32>::new()));
        let o2 = observed.clone();
        let m = monitor(s, move |x| o2.lock().push(*x));
        let collected = Sink::collect(m).await;
        assert_eq!(collected, vec![10, 20, 30]);
        assert_eq!(*observed.lock(), vec![10, 20, 30]);
    }

    #[tokio::test]
    async fn count_elements_totals_emitted_items() {
        let s = Source::from_iter(0..100i32);
        let (src, counter) = count_elements(s);
        let _ = Sink::collect(src).await;
        assert_eq!(counter.load(Ordering::Relaxed), 100);
    }
}
