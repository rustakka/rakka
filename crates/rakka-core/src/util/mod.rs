//! Core utilities. akka.net: `src/core/Akka/Util`.
//!
//! Small, focused helpers — BoundedQueue, monotonic clock, TypeId registry —
//! used throughout the actor subsystem.

mod bounded_queue;
mod clock;
mod snapshot;
mod type_registry;

pub use bounded_queue::BoundedQueue;
pub use clock::{MonotonicClock, SystemClock};
pub use snapshot::Snapshot;
pub use type_registry::TypeRegistry;
