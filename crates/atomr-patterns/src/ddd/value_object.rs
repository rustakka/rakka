//! [`ValueObject`] — equality-by-value, no identity.

use std::hash::Hash;

/// Marker trait for *value objects*: domain types that are immutable,
/// compared by value, and have no independent identity. Money, ranges,
/// addresses, codes — anything that, if you mutated it, would no longer
/// be the same value.
///
/// The trait carries no methods; its sole purpose is to make
/// "value-objectness" explicit at type sites and to bundle the standard
/// requirements (`Clone + Eq + Hash + Send + Sync + 'static`) into a
/// single bound.
pub trait ValueObject: Clone + Eq + Hash + Send + Sync + 'static {}
