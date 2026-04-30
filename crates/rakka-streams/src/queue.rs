//! Source.Queue — a source fed by an explicit producer handle.
//! akka.net: `ISourceQueue`, `Source.Queue`.

use tokio::sync::mpsc;

use crate::source::Source;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueOfferResult {
    Enqueued,
    Dropped,
    Failure,
    QueueClosed,
}

pub struct SourceQueue<T> {
    tx: mpsc::UnboundedSender<T>,
}

impl<T: Send + 'static> SourceQueue<T> {
    /// Create a source + producer pair. akka.net: `Source.Queue<T>(size, overflow)`.
    pub fn new() -> (Self, Source<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, Source::from_receiver(rx))
    }

    /// Offer an element synchronously. `QueueClosed` if the downstream stopped.
    pub fn offer(&self, value: T) -> QueueOfferResult {
        match self.tx.send(value) {
            Ok(()) => QueueOfferResult::Enqueued,
            Err(_) => QueueOfferResult::QueueClosed,
        }
    }

    /// Close the queue so the source completes normally.
    pub fn complete(self) {
        drop(self.tx);
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn source_queue_delivers_then_completes_on_drop() {
        let (q, src) = SourceQueue::<i32>::new();
        let handle = tokio::spawn(async move { Sink::collect(src).await });
        assert_eq!(q.offer(1), QueueOfferResult::Enqueued);
        assert_eq!(q.offer(2), QueueOfferResult::Enqueued);
        q.complete();
        assert_eq!(handle.await.unwrap(), vec![1, 2]);
    }
}
