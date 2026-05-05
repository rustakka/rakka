//! Fan-in and fan-out junctions.
//!
//! This port exposes the common linear-composition junctions without the
//! upstream graph-DSL plumbing: `merge`, `merge_all`, `concat`, `zip`,
//! `zip_with_index`, and `broadcast` (into two `Source<T>` clones).

use futures::stream::{select_all, StreamExt};

use crate::source::Source;

/// (interleaving, order not guaranteed).
pub fn merge<T: Send + 'static>(a: Source<T>, b: Source<T>) -> Source<T> {
    Source { inner: futures::stream::select(a.into_boxed(), b.into_boxed()).boxed() }
}

/// with arbitrary fan-in.
pub fn merge_all<T: Send + 'static, I: IntoIterator<Item = Source<T>>>(sources: I) -> Source<T> {
    let boxed = sources.into_iter().map(|s| s.into_boxed()).collect::<Vec<_>>();
    Source { inner: select_all(boxed).boxed() }
}

/// Drain first source fully, then second.
pub fn concat<T: Send + 'static>(a: Source<T>, b: Source<T>) -> Source<T> {
    a.concat(b)
}

/// Pair corresponding elements.
pub fn zip<A, B>(a: Source<A>, b: Source<B>) -> Source<(A, B)>
where
    A: Send + 'static,
    B: Send + 'static,
{
    Source { inner: a.into_boxed().zip(b.into_boxed()).boxed() }
}

/// Pair corresponding elements and apply `f`.
pub fn zip_with<A, B, C, F>(a: Source<A>, b: Source<B>, mut f: F) -> Source<C>
where
    A: Send + 'static,
    B: Send + 'static,
    C: Send + 'static,
    F: FnMut(A, B) -> C + Send + 'static,
{
    Source { inner: a.into_boxed().zip(b.into_boxed()).map(move |(x, y)| f(x, y)).boxed() }
}

pub fn zip_with_index<T: Send + 'static>(source: Source<T>) -> Source<(u64, T)> {
    Source { inner: source.into_boxed().enumerate().map(|(i, v)| (i as u64, v)).boxed() }
}

/// Merge two **already-sorted** sources
/// preserving total order. Both inputs must be ascending; output is
/// ascending. Buffers one element per side via tokio mpsc.
pub fn merge_sorted<T: Ord + Send + 'static>(a: Source<T>, b: Source<T>) -> Source<T> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<T>();
    let mut sa = a.into_boxed();
    let mut sb = b.into_boxed();
    tokio::spawn(async move {
        let mut head_a = sa.next().await;
        let mut head_b = sb.next().await;
        loop {
            match (head_a.take(), head_b.take()) {
                (None, None) => return,
                (Some(x), None) => {
                    if tx.send(x).is_err() {
                        return;
                    }
                    while let Some(rest) = sa.next().await {
                        if tx.send(rest).is_err() {
                            return;
                        }
                    }
                    return;
                }
                (None, Some(y)) => {
                    if tx.send(y).is_err() {
                        return;
                    }
                    while let Some(rest) = sb.next().await {
                        if tx.send(rest).is_err() {
                            return;
                        }
                    }
                    return;
                }
                (Some(x), Some(y)) => {
                    if x <= y {
                        if tx.send(x).is_err() {
                            return;
                        }
                        head_b = Some(y);
                        head_a = sa.next().await;
                    } else {
                        if tx.send(y).is_err() {
                            return;
                        }
                        head_a = Some(x);
                        head_b = sb.next().await;
                    }
                }
            }
        }
    });
    Source::from_receiver(rx)
}

/// Every input contributes elements in
/// proportion to its weight when both have items pending, falling
/// through to whichever side has work otherwise. Weights ≥ 1.
pub fn merge_prioritized<T: Send + 'static>(
    a: Source<T>,
    weight_a: u32,
    b: Source<T>,
    weight_b: u32,
) -> Source<T> {
    assert!(weight_a >= 1 && weight_b >= 1, "merge_prioritized weights must be >= 1");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<T>();
    let mut sa = a.into_boxed();
    let mut sb = b.into_boxed();
    tokio::spawn(async move {
        let mut budget_a = weight_a;
        let mut budget_b = weight_b;
        loop {
            tokio::select! {
                biased;
                ax = sa.next(), if budget_a > 0 => match ax {
                    Some(v) => {
                        if tx.send(v).is_err() { return; }
                        budget_a -= 1;
                        if budget_a == 0 && budget_b == 0 {
                            budget_a = weight_a;
                            budget_b = weight_b;
                        }
                    }
                    None => budget_a = 0,
                },
                bx = sb.next(), if budget_b > 0 => match bx {
                    Some(v) => {
                        if tx.send(v).is_err() { return; }
                        budget_b -= 1;
                        if budget_a == 0 && budget_b == 0 {
                            budget_a = weight_a;
                            budget_b = weight_b;
                        }
                    }
                    None => budget_b = 0,
                },
                else => return,
            }
        }
    });
    Source::from_receiver(rx)
}

/// Cheap fan-out into two independent sources
/// using cloned items and a bounded channel per downstream.
pub fn broadcast<T>(source: Source<T>) -> (Source<T>, Source<T>)
where
    T: Clone + Send + 'static,
{
    let (tx_a, rx_a) = tokio::sync::mpsc::unbounded_channel::<T>();
    let (tx_b, rx_b) = tokio::sync::mpsc::unbounded_channel::<T>();
    let mut inner = source.into_boxed();
    tokio::spawn(async move {
        while let Some(item) = inner.next().await {
            let _ = tx_a.send(item.clone());
            let _ = tx_b.send(item);
        }
    });
    (Source::from_receiver(rx_a), Source::from_receiver(rx_b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn merge_interleaves_two_sources() {
        let a = Source::from_iter(vec![1, 2, 3]);
        let b = Source::from_iter(vec![10, 20, 30]);
        let mut out = Sink::collect(merge(a, b)).await;
        out.sort();
        assert_eq!(out, vec![1, 2, 3, 10, 20, 30]);
    }

    #[tokio::test]
    async fn zip_pairs_sources() {
        let out =
            Sink::collect(zip(Source::from_iter(vec!["a", "b", "c"]), Source::from_iter(vec![1, 2, 3])))
                .await;
        assert_eq!(out, vec![("a", 1), ("b", 2), ("c", 3)]);
    }

    #[tokio::test]
    async fn zip_with_index_numbers_elements() {
        let out = Sink::collect(zip_with_index(Source::from_iter(vec!["x", "y"]))).await;
        assert_eq!(out, vec![(0, "x"), (1, "y")]);
    }

    #[tokio::test]
    async fn broadcast_duplicates_elements() {
        let (a, b) = broadcast(Source::from_iter(vec![1, 2, 3]));
        let (ra, rb) = tokio::join!(Sink::collect(a), Sink::collect(b));
        assert_eq!(ra, vec![1, 2, 3]);
        assert_eq!(rb, vec![1, 2, 3]);
    }
}
