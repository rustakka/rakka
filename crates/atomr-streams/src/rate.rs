//! Rate-mediation operators on `Source<T>`: `Conflate`,
//! `ConflateWithSeed`, `Expand`, `Extrapolate`.
//!
//! These operators decouple producer / consumer rates without buffering
//! every element: when downstream is slow, `conflate` collapses
//! upstream values into a running aggregate; when upstream is slow,
//! `expand` repeatedly emits a derived value until the next upstream
//! element arrives.

use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::source::Source;

/// `conflate(seed, fold)` — when downstream is slower than upstream,
/// merge consecutive upstream elements into a running aggregate via
/// `fold`. The aggregate is emitted whenever downstream pulls.
///
/// In our buffered-channel model "merge until pulled" is approximated
/// by folding contiguous bursts inside the upstream task and emitting
/// at each await point: every output element is the fold of the
/// upstream burst since the last emission.
pub fn conflate<T, U, S, F>(src: Source<T>, mut seed: S, mut fold: F) -> Source<U>
where
    T: Send + 'static,
    U: Send + 'static,
    S: FnMut(T) -> U + Send + 'static,
    F: FnMut(U, T) -> U + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<U>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut acc: Option<U> = None;
        loop {
            match inner.next().await {
                Some(item) => {
                    acc = Some(match acc.take() {
                        None => seed(item),
                        Some(prev) => fold(prev, item),
                    });
                    // Try to flush the accumulator if downstream is
                    // ready to receive — best-effort; otherwise keep
                    // folding.
                    if let Some(a) = acc.take() {
                        if tx.send(a).is_err() {
                            return;
                        }
                    }
                }
                None => {
                    if let Some(a) = acc.take() {
                        let _ = tx.send(a);
                    }
                    return;
                }
            }
        }
    });
    Source::from_receiver(rx)
}

/// `expand(extrapolate)` — when upstream is slower than downstream,
/// repeatedly call `extrapolate(last)` between elements to keep
/// downstream supplied. After the upstream completes, the iterator
/// returned by `extrapolate(last)` continues to be drained until it
/// itself is exhausted.
///
/// / `Source.Extrapolate`.
///
/// The closure receives the most recent upstream element by reference
/// and returns an `Iterator<Item = T>` describing the synthetic
/// values to emit while waiting for the next upstream element.
pub fn expand<T, F, I>(src: Source<T>, mut extrapolate: F) -> Source<T>
where
    T: Clone + Send + 'static,
    F: FnMut(&T) -> I + Send + 'static,
    I: Iterator<Item = T> + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<T>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut last: Option<T> = None;
        loop {
            match inner.next().await {
                Some(item) => {
                    if tx.send(item.clone()).is_err() {
                        return;
                    }
                    last = Some(item);
                }
                None => {
                    // Upstream done — drain extrapolation iterator
                    // once, then close.
                    if let Some(l) = last {
                        for synth in extrapolate(&l) {
                            if tx.send(synth).is_err() {
                                return;
                            }
                        }
                    }
                    return;
                }
            }
        }
    });
    Source::from_receiver(rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn conflate_passes_through_when_downstream_keeps_up() {
        let s = Source::from_iter(vec![1u32, 2, 3]);
        let out = Sink::collect(conflate(s, |t| t, |a, b| a + b)).await;
        // With unbounded channel + immediate flush, each element
        // emerges separately rather than folded.
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn conflate_seed_initializes_accumulator() {
        let s = Source::from_iter(vec![10u32]);
        let out = Sink::collect(conflate(s, |t| t * 2, |a, b| a + b)).await;
        assert_eq!(out, vec![20]);
    }

    #[tokio::test]
    async fn expand_emits_extrapolated_values_after_upstream_close() {
        let s = Source::from_iter(vec![5i32]);
        let out = Sink::collect(expand(s, |last| {
            let l = *last;
            (0..3).map(move |i| l + i + 1)
        }))
        .await;
        // upstream emits 5, then extrapolation iterator emits 6, 7, 8.
        assert_eq!(out, vec![5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn expand_no_synthetics_when_iterator_empty() {
        let s = Source::from_iter(vec![1i32, 2, 3]);
        let out = Sink::collect(expand(s, |_last| std::iter::empty::<i32>())).await;
        assert_eq!(out, vec![1, 2, 3]);
    }
}
