//! [`AggregateRoot`] — the transactional consistency boundary.
//!
//! Layers DDD identity + invariants over
//! [`atomr_persistence::Eventsourced`]. Every aggregate is event-sourced;
//! `AggregateRoot` adds:
//!
//! * a typed [`AggregateRoot::Id`] (the identity DDD requires),
//! * a default [`AggregateRoot::check_invariants`] hook for global
//!   post-apply checks the framework runs at strategic points
//!   (recommended: after every command's events are applied).
//!
//! The associated `Command` is required to implement
//! [`crate::Command`] with a matching `AggregateId`, so the framework
//! can route commands without dynamic dispatch.

use std::hash::Hash;

use atomr_persistence::Eventsourced;

/// DDD aggregate root. One per consistency boundary; every command and
/// every event passes through one of these.
///
/// **Note on bounds.** The matching constraints
/// `<Self as Eventsourced>::Command: Command<AggregateId = Self::Id>`
/// and `<Self as Eventsourced>::Event: DomainEvent` are *not* expressed
/// as a supertrait `where`-clause here, because Rust's MSRV-stable
/// trait machinery propagates such clauses awkwardly through every
/// usage site. The patterns that actually consume these bounds (e.g.
/// [`crate::cqrs::CqrsPattern`]) re-state them at their own builder /
/// impl sites. Do implement [`crate::Command`] for your `Command`
/// type and [`crate::DomainEvent`] for your `Event` type.
pub trait AggregateRoot: Eventsourced {
    /// The aggregate's identity type.
    type Id: Clone + Eq + Hash + Send + Sync + 'static;

    /// This instance's id.
    fn aggregate_id(&self) -> &Self::Id;

    /// Optional invariant check, run after applying a command's events
    /// to the in-memory state. Returning `Err` causes the framework to
    /// surface a [`crate::PatternError::Invariant`] *after* the events
    /// were persisted — i.e. it's a post-condition, not a guard. Use it
    /// to detect bugs in command handlers, not to gate writes (gate
    /// writes by returning `Err` from `command_to_events` instead).
    /// Default: always `Ok`.
    fn check_invariants(_state: &Self::State) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Encode `State` for snapshot persistence. Returning `None`
    /// disables snapshotting for this aggregate even if a snapshot
    /// store is configured at the pattern level — recovery falls back
    /// to journal replay only. Returning `Some(Err(_))` surfaces
    /// [`crate::PatternError::Codec`].
    fn encode_state(_state: &Self::State) -> Option<Result<Vec<u8>, String>> {
        None
    }

    /// Decode a snapshot payload back into `State`. Must be the
    /// inverse of [`Self::encode_state`]. Required iff `encode_state`
    /// is implemented.
    fn decode_state(_bytes: &[u8]) -> Result<Self::State, String> {
        Err("decode_state not implemented".into())
    }
}
