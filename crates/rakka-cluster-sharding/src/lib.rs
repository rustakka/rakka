//! rakka-cluster-sharding. akka.net: `src/contrib/cluster/Akka.Cluster.Sharding`.

mod coordinator;
mod entity_ref;
mod extractor;
mod shard;
mod shard_region;

pub use coordinator::ShardCoordinator;
pub use entity_ref::EntityRef;
pub use extractor::MessageExtractor;
pub use shard::Shard;
pub use shard_region::ShardRegion;
