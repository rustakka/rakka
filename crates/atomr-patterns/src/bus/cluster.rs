//! Cluster-wide [`super::DomainEventBus`] backed by
//! [`atomr_cluster_tools::ClusterPubSub`].
//!
//! Gated on the `bus-cluster` feature. The user supplies a
//! [`atomr_cluster_tools::MediatorTransport`] (the concrete
//! cross-process delivery mechanism) plus a typed event codec, and
//! the pattern wires up local fan-out + remote forwarding.
//!
//! ## Wiring
//!
//! ```ignore
//! let local = DistributedPubSub::new();
//! let cluster = ClusterPubSub::new(local.clone(), "node-a", transport);
//! let bus = DomainEventBus::<MyEvent>::builder()
//!     .name("orders")
//!     .cluster(local, cluster)
//!     .topic("orders")
//!     .type_id("MyEvent")
//!     .codec(|e| bincode::encode(e), |b| bincode::decode(b))
//!     .build()
//!     .materialize(&system).await?;
//! ```

use std::sync::Arc;

use atomr_cluster_tools::{ClusterPubSub, DistributedPubSub};

pub(crate) type EventEncoder<E> = Arc<dyn Fn(&E) -> Vec<u8> + Send + Sync + 'static>;
pub(crate) type EventDecoder<E> = Arc<dyn Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static>;

/// Bundled cluster wiring. Hold inside the bus topology when the
/// user opts into clustering.
pub(crate) struct ClusterConfig<E: Clone + Send + 'static> {
    /// Held to keep the local pubsub alive for the lifetime of the
    /// cluster bus; not directly read but ensures the decoder stays
    /// registered.
    #[allow(dead_code)]
    pub local: Arc<DistributedPubSub>,
    pub cluster: Arc<ClusterPubSub>,
    pub topic: String,
    pub type_id: String,
    pub encode: EventEncoder<E>,
    pub decode: EventDecoder<E>,
}
