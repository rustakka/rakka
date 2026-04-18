//! Source — the origin of elements in a stream graph.

use futures::stream::{self, BoxStream, StreamExt};

pub struct Source<T> {
    pub(crate) inner: BoxStream<'static, T>,
}

impl<T: Send + 'static> Source<T> {
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

    pub fn map<U, F>(self, f: F) -> Source<U>
    where
        F: FnMut(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        Source { inner: self.inner.map(f).boxed() }
    }

    pub fn filter<F>(self, mut f: F) -> Source<T>
    where
        F: FnMut(&T) -> bool + Send + 'static,
    {
        Source { inner: self.inner.filter(move |v| futures::future::ready(f(v))).boxed() }
    }

    pub fn take(self, n: usize) -> Source<T> {
        Source { inner: self.inner.take(n).boxed() }
    }

    pub fn via<U>(self, flow: crate::flow::Flow<T, U>) -> Source<U>
    where
        U: Send + 'static,
    {
        Source { inner: (flow.transform)(self.inner) }
    }
}
