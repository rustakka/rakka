//! GraphDsl — minimal builder mirroring `Akka.Streams.Dsl.GraphDsl`.
//!
//! This port exposes only the linear composition primitives needed by the
//! Source/Flow/Sink DSL; fan-in / fan-out junctions remain a follow-up.

use crate::flow::Flow;
use crate::sink::Sink;
use crate::source::Source;

pub struct GraphDsl;

impl GraphDsl {
    pub fn linear<A, B, C>(source: Source<A>, flow: Flow<A, B>) -> Source<B>
    where
        A: Send + 'static,
        B: Send + 'static,
        C: Send + 'static,
    {
        source.via(flow)
    }

    pub async fn run_fold<A, Acc, F>(source: Source<A>, init: Acc, f: F) -> Acc
    where
        A: Send + 'static,
        Acc: Send + 'static,
        F: FnMut(Acc, A) -> Acc + Send + 'static,
    {
        Sink::fold(source, init, f).await
    }
}
