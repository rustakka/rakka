//! `SourceRef[T]` / `SinkRef[T]` — handles to streams that can cross
//! process boundaries.
//!
//! Phase 12.9 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Streams.StreamRefs.{SourceRef, SinkRef}`. The wire-level
//! transport (sequence numbers, demand windows, retransmission) is
//! a follow-on; this module ships the in-process scaffolding that
//! lets a `Source<T>` be advertised over an mpsc channel and pulled
//! by a remote attacher.
//!
//! For Phase 5.D / Phase 6.D's wire integration, the channel handles
//! get serialized as `RemoteEnvelope`s; both ends use the same
//! `SourceRefHandle` shape so the local-only and cross-process
//! flavours share an API.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::source::Source;

/// Producer-side advertisement of a `Source<T>`. The owner pumps
/// elements; consumers subscribe via [`SourceRefHandle::take_source`].
pub struct SourceRefHandle<T: Send + 'static> {
    /// Globally-unique stream ref id (unique per node).
    pub id: u64,
    receiver: parking_lot::Mutex<Option<mpsc::Receiver<T>>>,
}

impl<T: Send + 'static> SourceRefHandle<T> {
    /// Advertise `source` as a stream ref. Returns the handle the
    /// caller serializes/sends to the consumer side.
    pub fn advertise(source: Source<T>, buffer: usize) -> Self {
        let id = next_ref_id();
        let buffer = buffer.max(1);
        let (tx, rx) = mpsc::channel::<T>(buffer);
        let mut inner = source.into_boxed();
        tokio::spawn(async move {
            while let Some(item) = inner.next().await {
                if tx.send(item).await.is_err() {
                    return;
                }
            }
        });
        Self { id, receiver: parking_lot::Mutex::new(Some(rx)) }
    }

    /// Take the consumer source. Calling more than once yields
    /// `Source::empty()` (the receiver only exists once).
    pub fn take_source(&self) -> Source<T> {
        match self.receiver.lock().take() {
            Some(rx) => Source { inner: rx_to_stream(rx).boxed() },
            None => Source::empty(),
        }
    }
}

fn rx_to_stream<T: Send + 'static>(rx: mpsc::Receiver<T>) -> futures::stream::BoxStream<'static, T> {
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) }).boxed()
}

/// Consumer-side advertisement of a `Sink<T>`. The producer attaches
/// a source via [`SinkRefHandle::attach`] which then pumps into the
/// receiver-owned stream.
pub struct SinkRefHandle<T: Send + 'static> {
    pub id: u64,
    sender: mpsc::Sender<T>,
    receiver: parking_lot::Mutex<Option<mpsc::Receiver<T>>>,
}

impl<T: Send + 'static> SinkRefHandle<T> {
    pub fn new(buffer: usize) -> Self {
        let buffer = buffer.max(1);
        let (tx, rx) = mpsc::channel::<T>(buffer);
        Self { id: next_ref_id(), sender: tx, receiver: parking_lot::Mutex::new(Some(rx)) }
    }

    /// Producer-side: attach `source` so its elements drain into the
    /// sink. Multiple attaches are merged.
    pub fn attach(&self, source: Source<T>) {
        let tx = self.sender.clone();
        let mut inner = source.into_boxed();
        tokio::spawn(async move {
            while let Some(item) = inner.next().await {
                if tx.send(item).await.is_err() {
                    return;
                }
            }
        });
    }

    /// Consumer-side: take the source that drains every attached
    /// producer.
    pub fn take_source(&self) -> Source<T> {
        match self.receiver.lock().take() {
            Some(rx) => Source { inner: rx_to_stream(rx).boxed() },
            None => Source::empty(),
        }
    }
}

fn next_ref_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

// `Arc` re-export so callers can pass handles between actors.
pub type SourceRef<T> = Arc<SourceRefHandle<T>>;
pub type SinkRef<T> = Arc<SinkRefHandle<T>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::time::Duration;

    #[tokio::test]
    async fn source_ref_round_trips_elements() {
        let s = Source::from_iter(vec![1, 2, 3, 4]);
        let handle: SourceRef<i32> = Arc::new(SourceRefHandle::advertise(s, 16));
        let consumed = Sink::collect(handle.take_source()).await;
        assert_eq!(consumed, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn source_ref_take_twice_yields_empty_second() {
        let s = Source::from_iter(vec![1]);
        let handle: SourceRef<i32> = Arc::new(SourceRefHandle::advertise(s, 1));
        let _ = handle.take_source();
        let v = tokio::time::timeout(Duration::from_millis(20), Sink::collect(handle.take_source()))
            .await
            .unwrap_or_default();
        assert!(v.is_empty());
    }

    #[tokio::test]
    async fn sink_ref_aggregates_attached_sources() {
        let sink: SinkRef<i32> = Arc::new(SinkRefHandle::new(16));
        sink.attach(Source::from_iter(vec![1, 2, 3]));
        sink.attach(Source::from_iter(vec![10, 20]));
        let merged = sink.take_source();
        // Drop the handle so its retained sender is released — without
        // this the merged source never sees `Closed` and we'd hang.
        drop(sink);
        let mut got = Sink::collect(merged).await;
        got.sort();
        assert_eq!(got, vec![1, 2, 3, 10, 20]);
    }

    #[tokio::test]
    async fn ref_ids_are_unique_per_node() {
        let s1: SourceRef<i32> = Arc::new(SourceRefHandle::advertise(Source::from_iter(vec![1]), 1));
        let s2: SourceRef<i32> = Arc::new(SourceRefHandle::advertise(Source::from_iter(vec![1]), 1));
        assert_ne!(s1.id, s2.id);
    }
}
