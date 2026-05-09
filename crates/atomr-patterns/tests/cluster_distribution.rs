//! Cluster bus end-to-end: two nodes, one shared in-memory transport,
//! event published on node A reaches subscribers on node B.

#![cfg(feature = "bus-cluster")]

use std::sync::Arc;
use std::time::Duration;

use atomr_cluster_tools::{ClusterPubSub, DistributedPubSub, MediatorPdu, MediatorTransport};
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::bus::DomainEventBus;
use atomr_patterns::topology::Topology;
use parking_lot::Mutex;

/// In-memory transport that hops PDUs between two named endpoints
/// registered into a shared switchboard.
#[derive(Default, Clone)]
struct LoopbackTransport {
    inner: Arc<Mutex<std::collections::HashMap<String, Arc<ClusterPubSub>>>>,
}

impl LoopbackTransport {
    fn register(&self, node: impl Into<String>, cluster: Arc<ClusterPubSub>) {
        self.inner.lock().insert(node.into(), cluster);
    }
}

impl MediatorTransport for LoopbackTransport {
    fn send(&self, target_node: &str, pdu: MediatorPdu) {
        let target = self.inner.lock().get(target_node).cloned();
        if let Some(c) = target {
            c.apply_pdu(pdu);
        }
    }
}

#[tokio::test]
async fn event_published_on_node_a_reaches_subscriber_on_node_b() {
    let system_a = ActorSystem::create("node-a", Config::reference()).await.unwrap();
    let system_b = ActorSystem::create("node-b", Config::reference()).await.unwrap();

    let transport: Arc<LoopbackTransport> = Arc::new(LoopbackTransport::default());
    let transport_arc: Arc<dyn MediatorTransport> = transport.clone();

    let local_a = DistributedPubSub::new();
    let local_b = DistributedPubSub::new();
    let cluster_a = ClusterPubSub::new(local_a.clone(), "node-a", transport_arc.clone());
    let cluster_b = ClusterPubSub::new(local_b.clone(), "node-b", transport_arc.clone());

    // Register the wrapped clusters in the switchboard so PDUs route
    // back into apply_pdu.
    transport.register("node-a", cluster_a.clone());
    transport.register("node-b", cluster_b.clone());

    // Build buses on both nodes with the same codec.
    let bus_a = DomainEventBus::<u32>::builder()
        .name("orders")
        .cluster(local_a.clone(), cluster_a.clone())
        .topic("orders")
        .type_id("u32")
        .codec(
            |e: &u32| e.to_le_bytes().to_vec(),
            |b: &[u8]| {
                let arr: [u8; 4] = b.try_into().map_err(|_| "len".to_string())?;
                Ok(u32::from_le_bytes(arr))
            },
        )
        .build()
        .materialize(&system_a)
        .await
        .unwrap();
    let bus_b = DomainEventBus::<u32>::builder()
        .name("orders")
        .cluster(local_b.clone(), cluster_b.clone())
        .topic("orders")
        .type_id("u32")
        .codec(
            |e: &u32| e.to_le_bytes().to_vec(),
            |b: &[u8]| {
                let arr: [u8; 4] = b.try_into().map_err(|_| "len".to_string())?;
                Ok(u32::from_le_bytes(arr))
            },
        )
        .build()
        .materialize(&system_b)
        .await
        .unwrap();

    // Subscribe on node B before announcing the topic to node A.
    let mut sub_b = bus_b.subscribe();

    // node-b advertises "orders" to node-a so node-a's cluster
    // pubsub knows to forward.
    cluster_b.announce_to("node-a");
    // Allow the in-memory transport to deliver the announce.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish on node A.
    bus_a.publish(42u32);

    let received = tokio::time::timeout(Duration::from_secs(1), sub_b.recv())
        .await
        .expect("timed out")
        .expect("subscriber closed");
    assert_eq!(received, 42);

    system_a.terminate().await;
    system_b.terminate().await;
}
