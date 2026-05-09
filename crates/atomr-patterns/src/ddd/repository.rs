//! [`Repository`] — public dispatch surface for commands.
//!
//! The `Repository` is what application code holds. It hides the
//! routing (which actor gets the command, whether it's local or
//! sharded) and surfaces a clean async fn the user invokes per command.

use async_trait::async_trait;

use atomr_persistence::Eventsourced;

use crate::{AggregateRoot, PatternError};

/// Send commands to an aggregate; receive the events that resulted (or
/// the typed domain error if the command was rejected).
///
/// Implementations are produced by `CqrsPattern::builder().build().materialize()`
/// — users don't normally implement this trait by hand.
#[async_trait]
pub trait Repository: Send + Sync {
    /// The aggregate this repository routes commands to.
    type Aggregate: AggregateRoot;

    /// Dispatch a command. Returns the events produced (and persisted)
    /// on success, or a [`PatternError`] wrapping the domain error / a
    /// transport / journal failure.
    async fn send(
        &self,
        cmd: <Self::Aggregate as Eventsourced>::Command,
    ) -> Result<
        Vec<<Self::Aggregate as Eventsourced>::Event>,
        PatternError<<Self::Aggregate as Eventsourced>::Error>,
    >;
}
