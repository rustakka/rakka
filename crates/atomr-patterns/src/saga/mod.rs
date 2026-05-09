//! Saga / Process Manager pattern.
//!
//! A [`Saga`] reacts to domain events and dispatches commands to drive
//! a long-running business process across multiple aggregates. State
//! is keyed by a correlation id derived from each event.

mod runner;
mod state_store;

pub use runner::{Saga, SagaAction, SagaHandles, SagaPattern, SagaTopology};
pub use state_store::{InMemorySagaStateStore, JournalSagaStateStore, SagaStateStore};
