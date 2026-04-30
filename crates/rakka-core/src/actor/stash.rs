//! Stash — buffers messages that should be deferred until `unstash_all`.
//! akka.net: `Actor/Stash/*`.
//!
//! The actual storage lives on [`crate::actor::Context`]; this module
//! provides a marker trait for symmetry with akka.net's `IWithStash`.

/// Marker — any actor may opt in to document stash usage.
/// Stash storage itself is provided unconditionally by `Context`.
pub trait Stash {}
