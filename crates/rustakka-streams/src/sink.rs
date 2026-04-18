//! Sink — consumes a `Source`, produces a materialized value.

use futures::stream::StreamExt;

use crate::source::Source;

pub struct Sink;

impl Sink {
    pub async fn fold<T, Acc, F>(source: Source<T>, init: Acc, mut f: F) -> Acc
    where
        T: Send + 'static,
        Acc: Send + 'static,
        F: FnMut(Acc, T) -> Acc + Send + 'static,
    {
        source.inner.fold(init, move |acc, x| futures::future::ready(f(acc, x))).await
    }

    pub async fn collect<T>(source: Source<T>) -> Vec<T>
    where
        T: Send + 'static,
    {
        source.inner.collect().await
    }

    pub async fn for_each<T, F>(source: Source<T>, mut f: F)
    where
        T: Send + 'static,
        F: FnMut(T) + Send + 'static,
    {
        source.inner.for_each(move |x| { f(x); futures::future::ready(()) }).await
    }

    pub async fn ignore<T: Send + 'static>(source: Source<T>) {
        source.inner.for_each(|_| futures::future::ready(())).await
    }
}
