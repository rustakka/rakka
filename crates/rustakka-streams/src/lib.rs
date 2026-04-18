//! rustakka-streams. akka.net: `src/core/Akka.Streams`.
//!
//! Source/Flow/Sink DSL built on top of `futures::Stream`. This is a
//! pragmatic port — we delegate the core pipeline execution to
//! `futures_util::StreamExt`, and expose ergonomic wrappers matching the
//! Akka.Streams names.

mod bidi;
mod flow;
mod graph;
mod materializer;
mod sink;
mod source;

pub use bidi::BidiFlow;
pub use flow::Flow;
pub use graph::GraphDsl;
pub use materializer::ActorMaterializer;
pub use sink::Sink;
pub use source::Source;
