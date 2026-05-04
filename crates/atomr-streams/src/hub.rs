//! Hub patterns: dynamic many-to-many fan-out / fan-in.
//!
//! Phase 12.5 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Streams.Dsl.BroadcastHub`, `MergeHub`. Hubs let consumers
//! attach to a live source (Broadcast) or producers attach to a live
//! sink (Merge) at runtime, after the graph has materialized.
//!
//! Built on `tokio::sync::broadcast` (BroadcastHub) and
//! `tokio::sync::mpsc` (MergeHub). The `BroadcastHub` buffer is
//! bounded; slow subscribers see lagged elements as silent gaps —
//! matching akka.net's `BroadcastHub.sink(bufferSize)` lag policy.

use futures::stream::{self, StreamExt};
use tokio::sync::{broadcast, mpsc};

use crate::source::Source;

// -- BroadcastHub --------------------------------------------------

/// Fan one source to many dynamic consumers.
pub struct BroadcastHub<T: Clone + Send + 'static> {
    sender: broadcast::Sender<T>,
}

impl<T: Clone + Send + 'static> BroadcastHub<T> {
    pub fn new(buffer_size: usize) -> Self {
        assert!(buffer_size >= 1, "buffer_size must be >= 1");
        let (sender, _rx) = broadcast::channel(buffer_size);
        Self { sender }
    }

    /// Attach a producer source. Spawns a task that pumps each
    /// element into the broadcast channel; returns immediately.
    pub fn attach(&self, source: Source<T>) {
        let tx = self.sender.clone();
        tokio::spawn(async move {
            let mut s = source.into_boxed();
            while let Some(item) = s.next().await {
                let _ = tx.send(item); // ok if no active receivers
            }
        });
    }

    /// Return a new consumer source. Yields elements broadcast after
    /// this call (late subscribers miss earlier elements). Slow
    /// subscribers silently skip lagged elements.
    pub fn consumer(&self) -> Source<T> {
        let rx = self.sender.subscribe();
        let stream = stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(item) => return Some((item, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Source { inner: stream.boxed() }
    }

    /// Number of currently-attached consumers.
    pub fn consumer_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

// -- MergeHub ------------------------------------------------------

/// Fan many dynamic producers into one consumer source.
pub struct MergeHub<T: Send + 'static> {
    sender: mpsc::UnboundedSender<T>,
    /// Held until [`MergeHub::source`] is called; then moved out.
    receiver: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<T>>>,
}

impl<T: Send + 'static> Default for MergeHub<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> MergeHub<T> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { sender: tx, receiver: parking_lot::Mutex::new(Some(rx)) }
    }

    /// Attach a producer source — pumped into the merged stream.
    pub fn attach(&self, source: Source<T>) {
        let tx = self.sender.clone();
        tokio::spawn(async move {
            let mut s = source.into_boxed();
            while let Some(item) = s.next().await {
                if tx.send(item).is_err() {
                    return;
                }
            }
        });
    }

    /// Take the merged consumer source. Calling more than once yields
    /// an empty source (the receiver only exists once).
    pub fn source(&self) -> Source<T> {
        match self.receiver.lock().take() {
            Some(rx) => Source::from_receiver(rx),
            None => Source::empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::time::Duration;

    #[tokio::test]
    async fn broadcast_hub_fans_to_two_consumers() {
        let hub = BroadcastHub::<i32>::new(16);
        let c1 = hub.consumer();
        let c2 = hub.consumer();

        // Attach AFTER subscribers so they don't miss elements.
        hub.attach(Source::from_iter(vec![1, 2, 3]));

        // Drop the hub so its retained sender is released — otherwise
        // consumers never observe `Closed` and would hang forever.
        drop(hub);

        // Both consumers see the same elements.
        let (a, b) = tokio::join!(Sink::collect(c1), Sink::collect(c2));
        assert_eq!(a, vec![1, 2, 3]);
        assert_eq!(b, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn broadcast_hub_late_consumer_misses_earlier_elements() {
        let hub = BroadcastHub::<i32>::new(16);
        // Pre-subscribe so the broadcast channel doesn't drop messages
        // before we measure the late subscriber.
        let c_pre = hub.consumer();
        hub.attach(Source::from_iter(vec![1, 2, 3]));
        // The hub keeps a sender alive, so `Sink::collect` would never
        // observe `Closed` — bound it with a timeout and check that we
        // received all three items.
        let pre = tokio::time::timeout(Duration::from_millis(200), async move {
            let mut got = Vec::new();
            let mut s = c_pre.into_boxed();
            while got.len() < 3 {
                match s.next().await {
                    Some(v) => got.push(v),
                    None => break,
                }
            }
            got
        })
        .await
        .unwrap_or_default();
        assert_eq!(pre, vec![1, 2, 3]);

        // Late consumer attaches after the source is exhausted → sees
        // nothing within the deadline.
        let c_late = hub.consumer();
        let late =
            tokio::time::timeout(Duration::from_millis(50), Sink::collect(c_late)).await.unwrap_or_default();
        assert!(late.is_empty());
    }

    #[tokio::test]
    async fn broadcast_hub_consumer_count_grows_with_subscribers() {
        let hub = BroadcastHub::<i32>::new(4);
        assert_eq!(hub.consumer_count(), 0);
        let _c1 = hub.consumer();
        let _c2 = hub.consumer();
        assert_eq!(hub.consumer_count(), 2);
    }

    #[tokio::test]
    async fn merge_hub_aggregates_multiple_producers() {
        let hub = MergeHub::<i32>::new();
        hub.attach(Source::from_iter(vec![1, 2, 3]));
        hub.attach(Source::from_iter(vec![10, 20, 30]));
        let merged = hub.source();
        // Drop the hub so the merged channel closes once attach tasks
        // finish — without this, `Sink::collect` waits forever.
        drop(hub);

        let mut got = Sink::collect(merged).await;
        got.sort();
        assert_eq!(got, vec![1, 2, 3, 10, 20, 30]);
    }

    #[tokio::test]
    async fn merge_hub_source_can_be_taken_only_once() {
        let hub = MergeHub::<i32>::new();
        hub.attach(Source::from_iter(vec![1]));
        let _ = hub.source();
        let s2 = hub.source();
        let v = tokio::time::timeout(Duration::from_millis(50), Sink::collect(s2)).await.unwrap_or_default();
        assert!(v.is_empty());
    }
}
