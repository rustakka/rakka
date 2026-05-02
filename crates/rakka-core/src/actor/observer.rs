//! Lifecycle observer hooks. Enables external crates (e.g.
//! `rakka-telemetry`) to observe actor spawn/stop/dead-letter events
//! without taking a dependency on `rakka-core`'s internals.

use super::path::ActorPath;

/// Implementors are notified whenever actors are spawned or stopped.
/// Methods are called on the actor's task, so they should be cheap and
/// non-blocking.
pub trait SpawnObserver: Send + Sync + 'static {
    fn on_spawn(&self, path: &ActorPath, parent: Option<&ActorPath>, actor_type: &'static str);
    fn on_stop(&self, path: &ActorPath);

    /// Called whenever the mailbox depth is sampled (optional).
    fn on_mailbox_depth(&self, _path: &ActorPath, _depth: u64) {}
}

/// Implementors are notified whenever a `tell` fails because the target
/// actor has stopped. Called on the caller's thread, so implementers
/// should be cheap.
pub trait DeadLetterObserver: Send + Sync + 'static {
    fn on_dead_letter(&self, recipient: &ActorPath, sender: Option<&ActorPath>, message_type: &'static str);
}
