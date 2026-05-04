//! Routing-junction operators on `Source<T>`.
//!
//! Phase 12.6 of `docs/full-port-plan.md`. Akka.NET parity:
//! `Partition`, `Balance`, `Unzip`, `Interleave`. Each consumes a
//! single source and exposes N downstream sources.

use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::source::Source;

/// `partition(n, f)` — fan one source into `n` output sources;
/// each element is sent to the output picked by `f(item)`.
/// Out-of-range outputs are dropped.
///
/// Akka.NET: `GraphDsl.Partition(n, fn)`.
pub fn partition<T, F>(src: Source<T>, n: usize, mut f: F) -> Vec<Source<T>>
where
    T: Send + 'static,
    F: FnMut(&T) -> usize + Send + 'static,
{
    assert!(n >= 1, "partition: n must be >= 1");
    let mut senders: Vec<mpsc::UnboundedSender<T>> = Vec::with_capacity(n);
    let mut sources: Vec<Source<T>> = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::unbounded_channel::<T>();
        senders.push(tx);
        sources.push(Source::from_receiver(rx));
    }
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        while let Some(item) = inner.next().await {
            let idx = f(&item);
            if let Some(tx) = senders.get(idx) {
                let _ = tx.send(item);
            }
            // out-of-range index → dropped
        }
        // senders drop here, closing each downstream
    });
    sources
}

/// `balance(n)` — round-robin one source into `n` outputs.
///
/// Akka.NET: `GraphDsl.Balance(n)`.
pub fn balance<T: Send + 'static>(src: Source<T>, n: usize) -> Vec<Source<T>> {
    assert!(n >= 1, "balance: n must be >= 1");
    let mut cursor = 0usize;
    partition(src, n, move |_item| {
        let i = cursor;
        cursor = (cursor + 1) % n;
        i
    })
}

/// `unzip(src)` — split a source of `(A, B)` pairs into two sources.
/// The second is buffered if the first lags (no backpressure across
/// the split — matches akka.net's fan-out semantics).
///
/// Akka.NET: `GraphDsl.Unzip<A, B>()`.
pub fn unzip<A, B>(src: Source<(A, B)>) -> (Source<A>, Source<B>)
where
    A: Send + 'static,
    B: Send + 'static,
{
    let (tx_a, rx_a) = mpsc::unbounded_channel::<A>();
    let (tx_b, rx_b) = mpsc::unbounded_channel::<B>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        while let Some((a, b)) = inner.next().await {
            let _ = tx_a.send(a);
            let _ = tx_b.send(b);
        }
    });
    (Source::from_receiver(rx_a), Source::from_receiver(rx_b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn partition_routes_by_function() {
        let s = Source::from_iter(vec![1, 2, 3, 4, 5, 6]);
        let mut outs = partition(s, 2, |x| (*x as usize) % 2);
        let evens = Sink::collect(outs.swap_remove(0)).await;
        let odds = Sink::collect(outs.swap_remove(0)).await;
        assert_eq!(evens, vec![2, 4, 6]);
        assert_eq!(odds, vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn balance_round_robins() {
        let s = Source::from_iter(vec![10, 20, 30, 40, 50, 60]);
        let mut outs = balance(s, 3);
        let c = Sink::collect(outs.swap_remove(2)).await;
        let b = Sink::collect(outs.swap_remove(1)).await;
        let a = Sink::collect(outs.swap_remove(0)).await;
        assert_eq!(a, vec![10, 40]);
        assert_eq!(b, vec![20, 50]);
        assert_eq!(c, vec![30, 60]);
    }

    #[tokio::test]
    async fn unzip_splits_pairs() {
        let s = Source::from_iter(vec![(1, "a"), (2, "b"), (3, "c")]);
        let (left, right) = unzip(s);
        let l = Sink::collect(left).await;
        let r = Sink::collect(right).await;
        assert_eq!(l, vec![1, 2, 3]);
        assert_eq!(r, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn partition_drops_out_of_range() {
        let s = Source::from_iter(vec![1, 2, 3]);
        let mut outs = partition(s, 1, |_| 99); // always out-of-range
        let only = Sink::collect(outs.swap_remove(0)).await;
        assert!(only.is_empty());
    }
}
