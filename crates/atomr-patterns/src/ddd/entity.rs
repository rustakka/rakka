//! [`Entity`] — anything with a stable identity.

use std::hash::Hash;

/// A domain object that is identified by its `Id`, not by its
/// attribute values. Two `Entity` instances with the same `Id` represent
/// the same conceptual thing even if their other fields differ.
pub trait Entity {
    /// The identity type. Must be hashable so the framework can index
    /// entities in maps (e.g. inside a [`crate::Repository`]).
    type Id: Clone + Eq + Hash + Send + Sync + 'static;

    /// Stable identity of this entity instance.
    fn id(&self) -> &Self::Id;
}
