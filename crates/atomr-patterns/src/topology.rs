//! [`Topology`] — common materialization surface for every pattern.
//!
//! Each `*Pattern::build()` produces a value implementing this trait.
//! Calling [`Topology::materialize`] spawns the pattern's actors under a
//! single named root in the user-guardian (`/user/<name>`) so the
//! dashboard's topology view renders the pattern as a cohesive subtree.

use async_trait::async_trait;

use atomr_core::actor::ActorSystem;

use crate::PatternError;

/// Inspectable, materializable description of a pattern's actor + stream
/// topology. Implementors hand back strongly-typed `Handles` after
/// materialization — you get a [`crate::Repository`], a
/// [`crate::cqrs::ProjectionHandle`], etc., depending on the pattern.
#[async_trait]
pub trait Topology: Send + 'static {
    /// The handle bundle returned after materialization.
    type Handles: Send + 'static;

    /// Spawn the pattern's actors and start its streams. Idempotent
    /// w.r.t. the returned handles, but each call to `materialize`
    /// spawns a fresh subtree — invoke it once per pattern instance.
    async fn materialize(self, system: &ActorSystem) -> Result<Self::Handles, PatternError<()>>;
}
