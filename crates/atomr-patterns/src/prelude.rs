//! Convenient re-exports.

pub use crate::cqrs::{CqrsHandles, CqrsPattern, ProjectionHandle, Reader};
pub use crate::ddd::{AggregateRoot, Command, DomainEvent, Entity, Repository, ValueObject};
pub use crate::saga::{Saga, SagaAction, SagaPattern};
pub use crate::topology::Topology;
pub use crate::PatternError;

pub use atomr_persistence::{
    AsyncSnapshotter, Eventsourced, EventsourcedError, InMemoryJournal, InMemorySnapshotStore,
    Journal, RecoveryPermitter, SnapshotPolicy, SnapshotStore,
};
pub use atomr_persistence_query::{EventEnvelope, Offset, ReadJournal};
