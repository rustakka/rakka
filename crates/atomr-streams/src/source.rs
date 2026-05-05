//! Source — the origin of elements in a stream graph.
//!
//! Implemented as a thin wrapper around a boxed [`futures::Stream`]; each
//! combinator returns a new `Source` whose inner stream lazily applies the
//! transformation. Matches the linear operator surface of `Dsl/Source.cs`
//! and `Dsl/SourceOperations.cs`.

use std::future::Future;
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};

use crate::flow::Flow;
use crate::overflow::OverflowStrategy;

pub struct Source<T> {
    pub(crate) inner: BoxStream<'static, T>,
}

impl<T: Send + 'static> Source<T> {
    // --- factories ---------------------------------

    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I: IntoIterator<Item = T> + Send + 'static>(iter: I) -> Self
    where
        I::IntoIter: Send + 'static,
    {
        Source { inner: stream::iter(iter).boxed() }
    }

    pub fn single(value: T) -> Self {
        Source { inner: stream::once(async move { value }).boxed() }
    }

    pub fn empty() -> Self {
        Source { inner: stream::empty().boxed() }
    }

    pub fn repeat(value: T) -> Self
    where
        T: Clone,
    {
        Source { inner: stream::repeat(value).boxed() }
    }

    pub fn cycle<I: IntoIterator<Item = T> + Clone + Send + 'static>(iter: I) -> Self
    where
        I::IntoIter: Send + 'static,
        T: Clone,
    {
        Source {
            inner: stream::unfold(iter.into_iter(), |mut it| async move { it.next().map(|v| (v, it)) })
                .boxed(),
        }
    }

    pub fn from_future<F>(fut: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        Source { inner: stream::once(fut).boxed() }
    }

    pub fn unfold<S, F, Fut>(init: S, f: F) -> Self
    where
        S: Send + 'static,
        F: FnMut(S) -> Fut + Send + 'static,
        Fut: Future<Output = Option<(T, S)>> + Send + 'static,
    {
        Source { inner: stream::unfold(init, f).boxed() }
    }

    pub fn tick(initial_delay: Duration, interval: Duration, value: T) -> Self
    where
        T: Clone,
    {
        let stream = stream::unfold(true, move |first| {
            let d = if first { initial_delay } else { interval };
            let v = value.clone();
            async move {
                tokio::time::sleep(d).await;
                Some((v, false))
            }
        });
        Source { inner: stream.boxed() }
    }

    pub fn failed<E>(error: E) -> Source<Result<T, E>>
    where
        E: Send + 'static,
    {
        Source { inner: stream::once(async move { Err(error) }).boxed() }
    }

    pub fn from_receiver(rx: tokio::sync::mpsc::UnboundedReceiver<T>) -> Self {
        Source { inner: stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|v| (v, rx)) }).boxed() }
    }

    // --- linear transforms -----------------------------------------------------

    pub fn map<U, F>(self, f: F) -> Source<U>
    where
        F: FnMut(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        Source { inner: self.inner.map(f).boxed() }
    }

    /// (ordered, bounded parallelism).
    pub fn map_async<U, F, Fut>(self, parallelism: usize, f: F) -> Source<U>
    where
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
    {
        let p = parallelism.max(1);
        Source { inner: self.inner.map(f).buffered(p).boxed() }
    }

    pub fn map_async_unordered<U, F, Fut>(self, parallelism: usize, f: F) -> Source<U>
    where
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = U> + Send + 'static,
        U: Send + 'static,
    {
        let p = parallelism.max(1);
        Source { inner: self.inner.map(f).buffer_unordered(p).boxed() }
    }

    /// `async_boundary(buffer)` — explicit async stage that decouples
    /// the upstream and downstream pipelines onto separate Tokio
    /// tasks via a bounded mpsc channel of capacity `buffer`.
    /// the `.async` call. Phase 12.3 of
    /// `docs/full-port-plan.md`.
    ///
    /// Useful when an upstream stage is CPU-heavy and you want
    /// downstream consumption to overlap with production. Slow
    /// downstream applies natural back-pressure once the buffer
    /// fills.
    pub fn async_boundary(self, buffer: usize) -> Source<T> {
        let buffer = buffer.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel::<T>(buffer);
        let mut inner = self.inner;
        tokio::spawn(async move {
            while let Some(item) = inner.next().await {
                if tx.send(item).await.is_err() {
                    return;
                }
            }
        });
        let stream =
            futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) });
        Source { inner: stream.boxed() }
    }

    pub fn filter<F>(self, mut f: F) -> Source<T>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        Source { inner: self.inner.filter(move |v| futures::future::ready(f(v))).boxed() }
    }

    pub fn filter_map<U, F>(self, mut f: F) -> Source<U>
    where
        F: FnMut(T) -> Option<U> + Send + 'static,
        U: Send + 'static,
    {
        Source { inner: self.inner.filter_map(move |v| futures::future::ready(f(v))).boxed() }
    }

    pub fn take(self, n: usize) -> Source<T> {
        Source { inner: self.inner.take(n).boxed() }
    }

    pub fn take_while<F>(self, mut f: F) -> Source<T>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        Source { inner: self.inner.take_while(move |v| futures::future::ready(f(v))).boxed() }
    }

    pub fn skip(self, n: usize) -> Source<T> {
        Source { inner: self.inner.skip(n).boxed() }
    }

    pub fn skip_while<F>(self, mut f: F) -> Source<T>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        Source { inner: self.inner.skip_while(move |v| futures::future::ready(f(v))).boxed() }
    }

    pub fn scan<Acc, F>(self, init: Acc, mut f: F) -> Source<Acc>
    where
        Acc: Clone + Send + 'static,
        F: FnMut(&Acc, T) -> Acc + Send + 'static,
    {
        Source {
            inner: self
                .inner
                .scan(init, move |state, item| {
                    *state = f(state, item);
                    futures::future::ready(Some(state.clone()))
                })
                .boxed(),
        }
    }

    /// Emit vectors of up to n items.
    pub fn grouped(self, n: usize) -> Source<Vec<T>> {
        Source { inner: self.inner.chunks(n.max(1)).boxed() }
    }

    pub fn intersperse(self, sep: T) -> Source<T>
    where
        T: Clone,
    {
        let state = InterspersState { started: false, pending: None, sep, done: false };
        Source {
            inner: stream::unfold((self.inner, state), |(mut s, mut st)| async move {
                if st.done {
                    return None;
                }
                if let Some(p) = st.pending.take() {
                    return Some((p, (s, st)));
                }
                let next = s.next().await;
                match next {
                    None => None,
                    Some(v) => {
                        if !st.started {
                            st.started = true;
                            Some((v, (s, st)))
                        } else {
                            st.pending = Some(v);
                            let sep = st.sep.clone();
                            Some((sep, (s, st)))
                        }
                    }
                }
            })
            .boxed(),
        }
    }

    pub fn concat(self, other: Source<T>) -> Source<T> {
        Source { inner: self.inner.chain(other.inner).boxed() }
    }

    pub fn prepend(self, other: Source<T>) -> Source<T> {
        Source { inner: other.inner.chain(self.inner).boxed() }
    }

    /// Shift every element by `d`.
    pub fn delay(self, d: Duration) -> Source<T> {
        Source {
            inner: self
                .inner
                .then(move |v| async move {
                    tokio::time::sleep(d).await;
                    v
                })
                .boxed(),
        }
    }

    /// Wait `d` before emitting the first element.
    pub fn initial_delay(self, d: Duration) -> Source<T> {
        let inner = self.inner;
        Source {
            inner: stream::once(async move {
                tokio::time::sleep(d).await;
                inner
            })
            .flatten()
            .boxed(),
        }
    }

    /// Limit element rate (one per `interval`).
    pub fn throttle(self, interval: Duration) -> Source<T> {
        Source {
            inner: self
                .inner
                .then(move |v| async move {
                    tokio::time::sleep(interval).await;
                    v
                })
                .boxed(),
        }
    }

    pub fn buffer(self, size: usize, strategy: OverflowStrategy) -> Source<T> {
        crate::overflow::apply(self, size, strategy)
    }

    /// Observes each element without affecting the stream.
    pub fn wire_tap<F>(self, mut f: F) -> Source<T>
    where
        F: FnMut(&T) + Send + 'static,
    {
        Source { inner: self.inner.inspect(move |v| f(v)).boxed() }
    }

    pub fn via<U>(self, flow: Flow<T, U>) -> Source<U>
    where
        U: Send + 'static,
    {
        Source { inner: (flow.transform)(self.inner) }
    }

    pub(crate) fn into_boxed(self) -> BoxStream<'static, T> {
        self.inner
    }
}

struct InterspersState<T> {
    started: bool,
    pending: Option<T>,
    sep: T,
    done: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn map_filter_take() {
        let out: Vec<i32> =
            Sink::collect(Source::from_iter(0..100).map(|x| x * 3).filter(|x| x % 2 == 0).take(5)).await;
        assert_eq!(out, vec![0, 6, 12, 18, 24]);
    }

    #[tokio::test]
    async fn scan_emits_running_state() {
        let out: Vec<i32> =
            Sink::collect(Source::from_iter(vec![1, 2, 3, 4]).scan(0, |acc, x| acc + x)).await;
        assert_eq!(out, vec![1, 3, 6, 10]);
    }

    #[tokio::test]
    async fn grouped_packs_chunks() {
        let out: Vec<Vec<i32>> = Sink::collect(Source::from_iter(1..=7).grouped(3)).await;
        assert_eq!(out, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
    }

    #[tokio::test]
    async fn intersperse_inserts_separator() {
        let out: Vec<i32> = Sink::collect(Source::from_iter(vec![1, 2, 3]).intersperse(0)).await;
        assert_eq!(out, vec![1, 0, 2, 0, 3]);
    }

    #[tokio::test]
    async fn map_async_preserves_order() {
        let out: Vec<i32> = Sink::collect(Source::from_iter(1..=4).map_async(4, |x| async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            x * x
        }))
        .await;
        assert_eq!(out, vec![1, 4, 9, 16]);
    }

    #[tokio::test]
    async fn concat_and_prepend_join_sources() {
        let a = Source::from_iter(vec![1, 2]);
        let b = Source::from_iter(vec![3, 4]);
        assert_eq!(Sink::collect(a.concat(b)).await, vec![1, 2, 3, 4]);

        let a = Source::from_iter(vec![1, 2]);
        let b = Source::from_iter(vec![3, 4]);
        assert_eq!(Sink::collect(a.prepend(b)).await, vec![3, 4, 1, 2]);
    }

    #[tokio::test]
    async fn wire_tap_observes_without_consuming() {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
        let seen_c = seen.clone();
        let out = Sink::collect(
            Source::from_iter(vec![1, 2, 3]).wire_tap(move |v| seen_c.lock().unwrap().push(*v)),
        )
        .await;
        assert_eq!(out, vec![1, 2, 3]);
        assert_eq!(seen.lock().unwrap().clone(), vec![1, 2, 3]);
    }
}
