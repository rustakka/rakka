//! rakka-cluster-sharding. akka.net: `src/contrib/cluster/Akka.Cluster.Sharding`.

mod allocation;
mod coordinator;
mod ddata_coordinator;
mod entity_ref;
mod extractor;
mod handoff;
mod passivation;
mod persistent_coordinator;
mod rebalance;
mod remember_entities;
mod shard;
mod shard_region;

pub use allocation::{LeastShardAllocationStrategy, PinnedAllocationStrategy, ShardAllocationStrategy};
pub use coordinator::ShardCoordinator;
pub use ddata_coordinator::DDataShardCoordinator;
pub use entity_ref::EntityRef;
pub use extractor::MessageExtractor;
pub use handoff::{HandoffCoordinator, HandoffError, HandoffState};
pub use passivation::PassivationTracker;
pub use persistent_coordinator::{
    project_into, CoordinatorCommand, CoordinatorError, CoordinatorEvent, CoordinatorState,
    PersistentShardCoordinator,
};
pub use rebalance::{RebalanceAction, RebalanceRunner};
pub use remember_entities::{
    InMemoryRememberStore, RememberEntitiesStore, RememberError, RememberedEntities,
};
pub use shard::Shard;
pub use shard_region::ShardRegion;
