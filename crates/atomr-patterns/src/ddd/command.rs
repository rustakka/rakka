//! [`Command`] — an intent to change one aggregate's state.

use std::hash::Hash;

/// A command targets exactly one [`crate::AggregateRoot`] (the
/// transactional consistency boundary). The framework needs to know
/// *which* aggregate before it can route the command, so every command
/// must surface its target id.
pub trait Command: Send + 'static {
    /// Identity type of the aggregate this command targets.
    type AggregateId: Clone + Eq + Hash + Send + Sync + 'static;

    /// Routing key — which aggregate instance should handle this command.
    fn aggregate_id(&self) -> Self::AggregateId;

    /// Optional sequence the caller expects the aggregate to be at
    /// when this command runs. When `Some(n)`, the gateway compares
    /// the entity's current sequence number to `n` and returns
    /// [`crate::PatternError::ConcurrencyConflict`] on mismatch — i.e.
    /// optimistic concurrency control. Default: `None` (no check).
    fn expected_version(&self) -> Option<u64> {
        None
    }

    /// Optional idempotency key. When the gateway has been configured
    /// with [`crate::cqrs::CqrsBuilder::dedupe_window`] (non-zero),
    /// commands with the same `command_id` for the same aggregate
    /// return the *previous* result without re-running the handler.
    /// Default: `None` (no dedupe).
    fn command_id(&self) -> Option<&str> {
        None
    }
}
