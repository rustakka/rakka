//! Phase 14.D — `cluster-pubsub-chat` example.
//!
//! Subscribers join a topic; one publisher broadcasts a few
//! messages; each subscriber sees every message. Demonstrates
//! `DistributedPubSub::publish_msg::<M>` typed broadcast and
//! `subscribe_to_group` round-robin delivery.
//!
//! Run with `cargo run -p example-cluster-pubsub-chat`.

use std::sync::Arc;
use std::time::Duration;

use atomr_cluster_tools::DistributedPubSub;
use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context, Props};

#[derive(Clone, Debug)]
struct ChatMsg {
    from: String,
    text: String,
}

struct Subscriber {
    name: String,
}

#[async_trait::async_trait]
impl Actor for Subscriber {
    type Msg = ChatMsg;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ChatMsg) {
        println!("[{}] {}: {}", self.name, msg.from, msg.text);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sys = ActorSystem::create("ChatSys", Config::reference()).await?;
    let bus = DistributedPubSub::new();

    // Two broadcast subscribers — both see every message.
    for name in ["alice", "bob"] {
        let n = name.to_string();
        let r = sys.actor_of(Props::create(move || Subscriber { name: n.clone() }), name)?;
        bus.subscribe("room1", r);
    }

    // Two group subscribers — only one sees each message (round-robin).
    for name in ["worker-a", "worker-b"] {
        let n = name.to_string();
        let r = sys.actor_of(Props::create(move || Subscriber { name: n.clone() }), name)?;
        bus.subscribe_to_group("work-queue", "G1", r);
    }

    // Broadcast to the room.
    for i in 1..=3 {
        let n = bus.publish_msg("room1", ChatMsg { from: "host".into(), text: format!("hello #{i}") });
        println!("(broadcast delivered to {n} subscribers)");
    }

    // Round-robin to the work-queue.
    for i in 1..=4 {
        let ok = bus.send_to_group(
            "work-queue",
            "G1",
            ChatMsg { from: "dispatcher".into(), text: format!("job-{i}") },
        );
        println!("(group send #{i} placed: {ok})");
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    sys.terminate().await;
    let _ = Arc::clone(&bus);
    Ok(())
}
