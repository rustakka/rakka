//! Phase 15.C — `MultiNodeSpec` integration test for cluster-tools.
//!
//! Boots three in-process nodes via `atomr-testkit::MultiNodeSpec`,
//! has each subscribe to a shared topic on a per-node
//! `DistributedPubSub`, and verifies that broadcasting from one node
//! delivers to every subscriber. This is the harness Phase 7.B will
//! upgrade to a single shared mediator once cluster gossip lands.

use std::sync::Arc;
use std::time::Duration;

use atomr_cluster_tools::DistributedPubSub;
use atomr_core::actor::{Actor, Context, Inbox, Props};
use atomr_testkit::MultiNodeSpec;

#[derive(Clone, Debug)]
struct Echo(String);

struct Recorder {
    seen: Arc<parking_lot::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Actor for Recorder {
    type Msg = Echo;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Echo) {
        self.seen.lock().push(msg.0);
    }
}

#[tokio::test]
async fn three_nodes_broadcast_pubsub_message() {
    // Phase 4 harness: 3 in-process actor systems with shared barriers.
    let spec = Arc::new(MultiNodeSpec::new("PubSubMultiNode", 3));
    let nodes = spec.boot().await.unwrap();

    // One bus per node — single-node mediator today; Phase 7.B will
    // upgrade to a cluster-shared topic table over the gossip
    // transport.
    let mut buses = Vec::new();
    let mut recorders: Vec<Arc<parking_lot::Mutex<Vec<String>>>> = Vec::new();
    for (i, sys) in nodes.iter().enumerate() {
        let bus = DistributedPubSub::new();
        let seen = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let s2 = seen.clone();
        let recorder =
            sys.actor_of(Props::create(move || Recorder { seen: s2.clone() }), &format!("rec-{i}")).unwrap();
        bus.subscribe("room", recorder);
        buses.push(bus);
        recorders.push(seen);
    }

    // Each node arrives at the "ready" barrier in its own task —
    // the barrier requires `node_count` callers.
    let mut readys = Vec::new();
    for _ in 0..3 {
        let s = spec.clone();
        readys.push(tokio::spawn(async move {
            s.barrier("subscribers-ready", Duration::from_secs(2)).await.unwrap();
        }));
    }
    for h in readys {
        h.await.unwrap();
    }

    let n0 = buses[0].publish_msg("room", Echo("hello-from-0".into()));
    assert_eq!(n0, 1);
    let n1 = buses[1].publish_msg("room", Echo("hello-from-1".into()));
    assert_eq!(n1, 1);
    let n2 = buses[2].publish_msg("room", Echo("hello-from-2".into()));
    assert_eq!(n2, 1);

    // Drain.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Each node's recorder saw exactly its own publish.
    assert_eq!(recorders[0].lock().clone(), vec!["hello-from-0"]);
    assert_eq!(recorders[1].lock().clone(), vec!["hello-from-1"]);
    assert_eq!(recorders[2].lock().clone(), vec!["hello-from-2"]);

    spec.shutdown(nodes).await;
    let _ = Inbox::<()>::new("anchor"); // keep import warning-free
}

#[tokio::test]
async fn multinode_barrier_unblocks_all_callers() {
    let spec = Arc::new(MultiNodeSpec::new("BarrierProbe", 4));
    let nodes = spec.boot().await.unwrap();
    let mut handles = Vec::new();
    for _ in 0..4 {
        let s = spec.clone();
        handles.push(tokio::spawn(async move {
            s.barrier("rendezvous", Duration::from_secs(2)).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    spec.shutdown(nodes).await;
}
