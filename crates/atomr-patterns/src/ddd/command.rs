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
}
