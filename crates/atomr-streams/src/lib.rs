//! atomr-streams. akka.net: `src/core/Akka.Streams`.
//!
//! Source/Flow/Sink DSL built on top of `futures::Stream`. The surface
//! covers the linear operator set and the most common graph-DSL
//! junctions from upstream:
//!
//! * [`Source`], [`Flow`], [`Sink`] — core linear elements.
//! * [`graph`] — `merge`, `broadcast`, `zip`, `concat` junctions.
//! * [`Framing`] — delimiter / length-field byte framing.
//! * [`FileIO`], [`Tcp`] — I/O adapters.
//! * [`KillSwitch`] — external termination.
//! * [`RestartSource`] / [`RestartSettings`] — automatic resubscription.
//! * [`SourceQueue`] / [`Sink::queue`] — explicit backpressure handles.
//! * [`OverflowStrategy`] — bounded-buffer policies.
//! * [`BidiFlow`] — bidirectional composition.
//!
//! The port delegates pipeline execution to `futures_util::StreamExt`; each
//! combinator builds a boxed stream closure that mirrors the corresponding
//! Akka.Streams operator.

mod bidi;
mod file_io;
mod flow;
mod framing;
mod graph;
mod hub;
mod junction;
mod kill_switch;
mod lifecycle;
mod materializer;
mod overflow;
mod queue;
mod recovery;
mod restart;
mod routing;
mod runnable;
mod sink;
mod source;
mod stream_ref;
mod substream;
mod supervision;
mod tcp;
mod timed;

pub use bidi::BidiFlow;
pub use file_io::FileIO;
pub use flow::Flow;
pub use framing::{Framing, FramingError};
pub use graph::GraphDsl;
pub use hub::{BroadcastHub, MergeHub};
pub use junction::{broadcast, concat, merge, merge_all, zip, zip_with, zip_with_index};
pub use kill_switch::KillSwitch;
pub use lifecycle::{count_elements, monitor, watch_termination};
pub use materializer::ActorMaterializer;
pub use overflow::OverflowStrategy;
pub use queue::{QueueOfferResult, SourceQueue};
pub use recovery::{map_error, recover, recover_with};
pub use restart::{RestartSettings, RestartSource};
pub use routing::{balance, partition, unzip};
pub use runnable::RunnableGraph;
pub use sink::{Sink, SinkQueue};
pub use source::Source;
pub use stream_ref::{SinkRef, SinkRefHandle, SourceRef, SourceRefHandle};
pub use substream::{group_by, split_when};
pub use supervision::{deciders, with_decider, Decider, SupervisionDirective};
pub use tcp::{IncomingConnection, OutgoingConnection, Tcp};
pub use timed::{grouped_within, idle_timeout};
