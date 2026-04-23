//! RunnableGraph — a `Source` + terminal `Sink` waiting to be materialized.
//! akka.net: `Dsl/RunnableGraph.cs`.

use std::future::Future;

use crate::source::Source;

pub struct RunnableGraph<M> {
    runner: Box<dyn FnOnce() -> futures::future::BoxFuture<'static, M> + Send + 'static>,
}

impl<M: Send + 'static> RunnableGraph<M> {
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = M> + Send + 'static,
    {
        use futures::FutureExt;
        RunnableGraph {
            runner: Box::new(move || f().boxed()),
        }
    }

    pub async fn run(self) -> M {
        (self.runner)().await
    }
}

impl<T: Send + 'static> RunnableGraph<Vec<T>> {
    pub fn to_seq(source: Source<T>) -> Self {
        Self::new(move || crate::sink::Sink::collect(source))
    }
}
