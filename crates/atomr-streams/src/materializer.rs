//! ActorMaterializer — runs graphs on a Tokio runtime.

use std::future::Future;

use crate::runnable::RunnableGraph;
use crate::sink::Sink;
use crate::source::Source;

#[derive(Default, Clone)]
pub struct ActorMaterializer;

impl ActorMaterializer {
    pub fn new() -> Self {
        Self
    }

    /// Convenience: run a source into a collecting sink and return the result.
    pub async fn run_collect<T: Send + 'static>(&self, source: Source<T>) -> Vec<T> {
        Sink::collect(source).await
    }

    /// Run an arbitrary `RunnableGraph`.
    pub async fn run<M: Send + 'static>(&self, graph: RunnableGraph<M>) -> M {
        graph.run().await
    }

    /// Run a source against an arbitrary async terminator.
    pub async fn run_with<T, F, Fut, Out>(&self, source: Source<T>, terminator: F) -> Out
    where
        T: Send + 'static,
        F: FnOnce(Source<T>) -> Fut,
        Fut: Future<Output = Out>,
    {
        terminator(source).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::Flow;

    #[tokio::test]
    async fn map_and_collect_pipeline() {
        let mat = ActorMaterializer::new();
        let source = Source::from_iter(vec![1, 2, 3, 4]);
        let flow: Flow<i32, i32> = Flow::from_fn(|x| x * 2);
        let result = mat.run_collect(source.via(flow)).await;
        assert_eq!(result, vec![2, 4, 6, 8]);
    }

    #[tokio::test]
    async fn fold_via_sink() {
        let source = Source::from_iter(1..=5i32);
        let sum = Sink::fold(source, 0, |acc, x| acc + x).await;
        assert_eq!(sum, 15);
    }

    #[tokio::test]
    async fn runnable_graph_to_seq() {
        let mat = ActorMaterializer::new();
        let graph = RunnableGraph::to_seq(Source::from_iter(vec![10, 20, 30]));
        assert_eq!(mat.run(graph).await, vec![10, 20, 30]);
    }
}
