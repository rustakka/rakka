//! Chat example. akka.net: `src/examples/Cluster.Tools.Chat`.
//!
//! Single-process demonstration of DistributedPubSub.

use std::sync::Arc;

use async_trait::async_trait;
use rustakka::prelude::*;
use rustakka_cluster_tools::DistributedPubSub;

#[derive(Debug, Clone)]
enum ChatMsg {
    Post(String),
}

struct Participant {
    name: String,
    bus: Arc<DistributedPubSub>,
    topic: String,
}

#[async_trait]
impl Actor for Participant {
    type Msg = ChatMsg;

    async fn pre_start(&mut self, ctx: &mut Context<Self>) {
        self.bus.subscribe(self.topic.clone(), ctx.self_ref().as_untyped());
    }

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ChatMsg) {
        match msg {
            ChatMsg::Post(text) => println!("[{}] got: {}", self.name, text),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sys = ActorSystem::create("chat", Config::empty()).await?;
    let bus = DistributedPubSub::new();

    let alice = sys.actor_of(
        Props::create({
            let bus = bus.clone();
            move || Participant {
                name: "alice".into(),
                bus: bus.clone(),
                topic: "room1".into(),
            }
        }),
        "alice",
    )?;

    let _bob = sys.actor_of(
        Props::create({
            let bus = bus.clone();
            move || Participant {
                name: "bob".into(),
                bus: bus.clone(),
                topic: "room1".into(),
            }
        }),
        "bob",
    )?;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    alice.tell(ChatMsg::Post("hello room".into()));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    println!("subscribers in room1: {}", bus.publish("room1").len());
    sys.terminate().await;
    Ok(())
}
