//! Routers — distribute messages across a pool of routees.
//! akka.net: `src/core/Akka/Routing/`.
//!
//! We model a "routee" as any [`crate::actor::ActorRef<M>`] for a common
//! message type `M`. Each router type carries routing state and exposes a
//! single `route(msg)` entry point. For brevity here we expose 6 routing
//! logics — the full akka.net set.

mod broadcast;
mod consistent_hash;
mod random;
mod round_robin;
mod scatter_gather;
mod smallest_mailbox;

pub use broadcast::BroadcastRouter;
pub use consistent_hash::ConsistentHashRouter;
pub use random::RandomRouter;
pub use round_robin::RoundRobinRouter;
pub use scatter_gather::ScatterGatherFirstCompletedRouter;
pub use smallest_mailbox::SmallestMailboxRouter;
