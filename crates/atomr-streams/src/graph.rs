//! GraphDsl — minimal builder for fan-in / fan-out stream graphs.
//!
//! Linear composition lives on `Source::via`; this module collects the
//! fan-in / fan-out junctions so callers can assemble a linear-plus-junction
//! graph without needing the full upstream graph-DSL runtime.

use crate::flow::Flow;
use crate::sink::Sink;
use crate::source::Source;

pub struct GraphDsl;

impl GraphDsl {
    pub fn linear<A, B>(source: Source<A>, flow: Flow<A, B>) -> Source<B>
    where
        A: Send + 'static,
        B: Send + 'static,
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
