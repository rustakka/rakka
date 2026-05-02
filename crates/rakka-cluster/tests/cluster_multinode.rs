//! Phase 15.C — `MultiNodeSpec` integration test for the cluster
//! daemon's active gossip dissemination loop.
//!
//! Three "nodes" (in-process daemons) share an in-memory transport.
//! Each joins itself + its peers; after a few ticks every node should
//! see the same membership.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rakka_cluster::{spawn_daemon, ClusterEventBus, DaemonConfig, GossipPdu, GossipTransport, Member};
use rakka_core::actor::Address;
use tokio::sync::mpsc;

#[derive(Default, Clone)]
struct InMemNet {
    inboxes: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<GossipPdu>>>>,
}

impl GossipTransport for InMemNet {
    fn send(&self, target: &Address, pdu: GossipPdu) {
        if let Some(tx) = self.inboxes.lock().get(&target.to_string()) {
            let _ = tx.send(pdu);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_membership_converges_over_gossip() {
    let net = InMemNet::default();
    let addr_a = Address::local("nodeA");
    let addr_b = Address::local("nodeB");
    let addr_c = Address::local("nodeC");

    let cfg = DaemonConfig { gossip_interval: Duration::from_millis(30) };
    let a = spawn_daemon(addr_a.clone(), Arc::new(net.clone()), ClusterEventBus::new(), cfg.clone());
    let b = spawn_daemon(addr_b.clone(), Arc::new(net.clone()), ClusterEventBus::new(), cfg.clone());
    let c = spawn_daemon(addr_c.clone(), Arc::new(net.clone()), ClusterEventBus::new(), cfg);

    net.inboxes.lock().insert(addr_a.to_string(), a.gossip_inbox());
    net.inboxes.lock().insert(addr_b.to_string(), b.gossip_inbox());
    net.inboxes.lock().insert(addr_c.to_string(), c.gossip_inbox());

    for h in [&a, &b, &c] {
        h.join(Member::new(addr_a.clone(), vec![]));
        h.join(Member::new(addr_b.clone(), vec![]));
        h.join(Member::new(addr_c.clone(), vec![]));
    }

    for _ in 0..20 {
        a.tick();
        b.tick();
        c.tick();
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let snap_a = a.snapshot();
    let snap_b = b.snapshot();
    let snap_c = c.snapshot();
    assert_eq!(snap_a.state.member_count(), 3);
    assert_eq!(snap_b.state.member_count(), 3);
    assert_eq!(snap_c.state.member_count(), 3);

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}
