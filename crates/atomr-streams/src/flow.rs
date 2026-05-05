//! Flow — a linear transformation from `In` to `Out`.
//!
//! A `Flow<A, B>` is a boxed closure that turns a `Stream<A>` into a
//! `Stream<B>`. Composition is by function chaining, which mirrors the
//! semantics of `Dsl/FlowOperations.cs` for the linear subset of
//! operators we provide.

use std::future::Future;
use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};

pub struct Flow<In, Out> {
    pub(crate) transform: Box<dyn FnOnce(BoxStream<'static, In>) -> BoxStream<'static, Out> + Send + 'static>,
}

impl<T: Send + 'static> Flow<T, T> {
    pub fn identity() -> Self {
        Flow { transform: Box::new(|s| s) }
    }
}

impl<In: Send + 'static, Out: Send + 'static> Flow<In, Out> {
    /// Pure synchronous mapping./ `Select`.
    pub fn from_fn<F>(f: F) -> Self
    where
        F: FnMut(In) -> Out + Send + 'static,
    {
        Flow { transform: Box::new(move |s: BoxStream<'static, In>| s.map(f).boxed()) }
    }

    /// Asynchronous mapping with ordered bounded parallelism.
    pub fn map_async<F, Fut>(parallelism: usize, f: F) -> Self
    where
        F: FnMut(In) -> Fut + Send + 'static,
        Fut: Future<Output = Out> + Send + 'static,
    {
        let p = parallelism.max(1);
        Flow { transform: Box::new(move |s: BoxStream<'static, In>| s.map(f).buffered(p).boxed()) }
    }

    /// Chain another flow after this one.
    pub fn via<Out2: Send + 'static>(self, next: Flow<Out, Out2>) -> Flow<In, Out2> {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                let mid = (self.transform)(s);
                (next.transform)(mid)
            }),
        }
    }

    /// Compose with a post-processing closure./ `Select`.
    pub fn then<Out2, F>(self, g: F) -> Flow<In, Out2>
    where
        Out2: Send + 'static,
        F: FnMut(Out) -> Out2 + Send + 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                let out = (self.transform)(s);
                out.map(g).boxed()
            }),
        }
    }
}

impl<In: Send + 'static> Flow<In, In> {
    pub fn filter<F>(mut f: F) -> Self
    where
        F: FnMut(&In) -> bool + Send + 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                s.filter(move |v| futures::future::ready(f(v))).boxed()
            }),
        }
    }

    pub fn take(n: usize) -> Self {
        Flow { transform: Box::new(move |s: BoxStream<'static, In>| s.take(n).boxed()) }
    }

    pub fn skip(n: usize) -> Self {
        Flow { transform: Box::new(move |s: BoxStream<'static, In>| s.skip(n).boxed()) }
    }

    pub fn throttle(interval: Duration) -> Self {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                s.then(move |v| async move {
                    tokio::time::sleep(interval).await;
                    v
                })
                .boxed()
            }),
        }
    }
}

impl<In: Send + 'static, Out: Send + 'static> Flow<In, Out> {
    /// / `flatMapConcat`.
    pub fn flat_map_concat<F, S, U>(mut f: F) -> Flow<In, U>
    where
        F: FnMut(In) -> S + Send + 'static,
        S: IntoIterator<Item = U> + Send + 'static,
        S::IntoIter: Send + 'static,
        U: Send + 'static,
        // Keep Out type linked for inference; unused here.
        In: 'static,
    {
        Flow {
            transform: Box::new(move |s: BoxStream<'static, In>| {
                s.flat_map(move |x| futures::stream::iter(f(x))).boxed()
            }),
        }
    }
}
