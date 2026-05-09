//! CQRS + Event Sourcing pattern.
//!
//! `CqrsPattern` wires up the four moving parts of a typical
//! command-query stack:
//!
//! 1. A **command gateway** actor that owns one
//!    [`crate::AggregateRoot`] instance per aggregate id and persists
//!    events through the configured [`atomr_persistence::Journal`].
//! 2. A **repository** handle that callers use to dispatch commands.
//! 3. Zero or more **readers** — async tasks that follow the
//!    [`atomr_persistence_query::ReadJournal`], decode events with the
//!    user-supplied codec, and fold them into projection state.
//! 4. **Extension hooks** — pre-handler interceptors (validation,
//!    authorization), post-persist event listeners, and async event
//!    taps that bridge to [`atomr_streams`] / external systems.
//!
//! See [`CqrsPattern::builder`] for the entry point.

pub mod audit;
mod builder;
mod command_gateway;
mod event_codec;
mod projection;
mod reader;
mod scheduled;

pub use audit::{AuditLog, AuditProjection};
pub use builder::{CqrsBuilder, CqrsHandles, CqrsPattern, CqrsTopology};
pub use event_codec::EventCodecRegistry;
pub use projection::ProjectionHandle;
pub use reader::{Reader, ReaderFilter};
pub use scheduled::schedule_command;
