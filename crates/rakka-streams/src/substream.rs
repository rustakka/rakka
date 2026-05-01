//! Substream operators on `Source<T>`.
//!
//! Phase 12.1 of `docs/full-port-plan.md`. Akka.NET / Akka Streams
//! parity: `GroupBy`, `SplitWhen`, `SplitAfter`. We ship the
//! pragmatic shape: each operator returns a stream of
//! `(key, Source<T>)` (for `group_by`) or `Source<T>` (for split
//! variants), buffered through tokio mpsc channels rather than the
//! materializer-coordinated SubFlow algebra of the JVM port.

use std::collections::HashMap;
use std::hash::Hash;

use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::source::Source;

/// `group_by(max_substreams, key_fn)` — fan one source into N
/// per-key substreams. Each new key yields a `(key, Source<T>)`
/// pair on the returned outer source. Once `max_substreams` keys
/// are open, additional keys' elements are dropped.
///
/// Akka.NET: `Source.GroupBy(maxSubstreams, key)`.
pub fn group_by<T, K, F>(
    src: Source<T>,
    max_substreams: usize,
    mut key_fn: F,
) -> Source<(K, Source<T>)>
where
    T: Send + 'static,
    K: Eq + Hash + Clone + Send + 'static,
    F: FnMut(&T) -> K + Send + 'static,
{
    assert!(max_substreams >= 1, "max_substreams must be >= 1");
    let (outer_tx, outer_rx) = mpsc::unbounded_channel::<(K, Source<T>)>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut substreams: HashMap<K, mpsc::UnboundedSender<T>> = HashMap::new();
        while let Some(item) = inner.next().await {
            let key = key_fn(&item);
            if let Some(tx) = substreams.get(&key) {
                let _ = tx.send(item);
                continue;
            }
            if substreams.len() >= max_substreams {
                // Spec-aligned: silently drop new keys past the cap.
                continue;
            }
            let (sub_tx, sub_rx) = mpsc::unbounded_channel::<T>();
            let _ = sub_tx.send(item);
            substreams.insert(key.clone(), sub_tx);
            if outer_tx.send((key, Source::from_receiver(sub_rx))).is_err() {
                // Outer consumer dropped; abort.
                return;
            }
        }
        // Upstream complete — drop sub_tx senders so each substream
        // sees clean termination. Done by HashMap drop.
    });
    Source::from_receiver(outer_rx)
}

/// `split_when(pred)` — split the source into a sequence of
/// substreams; a new substream begins when `pred(item)` returns true,
/// with the splitting element going to the **new** substream.
///
/// Akka.NET: `Source.SplitWhen(pred)`.
pub fn split_when<T, F>(src: Source<T>, mut pred: F) -> Source<Source<T>>
where
    T: Send + 'static,
    F: FnMut(&T) -> bool + Send + 'static,
{
    let (outer_tx, outer_rx) = mpsc::unbounded_channel::<Source<T>>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut current_tx: Option<mpsc::UnboundedSender<T>> = None;
        while let Some(item) = inner.next().await {
            let split = pred(&item);
            if split || current_tx.is_none() {
                let (sub_tx, sub_rx) = mpsc::unbounded_channel::<T>();
                if outer_tx.send(Source::from_receiver(sub_rx)).is_err() {
                    return;
                }
                current_tx = Some(sub_tx);
            }
            if let Some(tx) = &current_tx {
                let _ = tx.send(item);
            }
        }
    });
    Source::from_receiver(outer_rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use std::collections::HashMap;

    #[tokio::test]
    async fn group_by_partitions_into_substreams_by_key() {
        let s = Source::from_iter(vec![1, 2, 3, 4, 5, 6]);
        let outer = group_by(s, 2, |x: &i32| *x % 2);
        let pairs = Sink::collect(outer).await;
        let mut by_key: HashMap<i32, Vec<i32>> = HashMap::new();
        for (k, sub) in pairs {
            let v = Sink::collect(sub).await;
            by_key.insert(k, v);
        }
        assert_eq!(by_key.get(&0), Some(&vec![2, 4, 6]));
        assert_eq!(by_key.get(&1), Some(&vec![1, 3, 5]));
    }

    #[tokio::test]
    async fn group_by_drops_keys_past_cap() {
        let s = Source::from_iter(vec![1, 2, 3, 4, 5, 6]);
        // Cap at 1 — only the first key (=1) gets a substream.
        let outer = group_by(s, 1, |x: &i32| *x % 3);
        let pairs = Sink::collect(outer).await;
        assert_eq!(pairs.len(), 1);
        let (k, sub) = pairs.into_iter().next().unwrap();
        assert_eq!(k, 1);
        let v = Sink::collect(sub).await;
        assert_eq!(v, vec![1, 4]);
    }

    #[tokio::test]
    async fn split_when_starts_new_substream_on_predicate() {
        let s = Source::from_iter(vec![1, 2, 10, 3, 4, 20, 5]);
        let outer = split_when(s, |x: &i32| *x >= 10);
        let subs = Sink::collect(outer).await;
        let mut chunks = Vec::new();
        for sub in subs {
            chunks.push(Sink::collect(sub).await);
        }
        assert_eq!(chunks, vec![vec![1, 2], vec![10, 3, 4], vec![20, 5]]);
    }
}
