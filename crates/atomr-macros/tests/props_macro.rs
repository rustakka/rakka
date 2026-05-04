//! Phase 1.E — `props!` macro.
//!
//! Verifies that `props!(Foo { ... })` expands to a `Props<Foo>` that
//! can be passed to `ActorSystem::actor_of`.

use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context};
use atomr_macros::props;
use std::time::Duration;
use tokio::sync::oneshot;

struct Greeter {
    prefix: String,
}

enum GreeterMsg {
    Hello(String, oneshot::Sender<String>),
}

#[async_trait::async_trait]
impl Actor for Greeter {
    type Msg = GreeterMsg;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: GreeterMsg) {
        match msg {
            GreeterMsg::Hello(name, reply) => {
                let _ = reply.send(format!("{}, {}", self.prefix, name));
            }
        }
    }
}

#[tokio::test]
async fn props_macro_creates_actor() {
    let sys = ActorSystem::create("PropsMacro", Config::reference()).await.unwrap();
    let greeter = sys.actor_of(props!(Greeter { prefix: "hi".into() }), "g").unwrap();
    let reply = greeter
        .ask_with(|tx| GreeterMsg::Hello("world".into(), tx), Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(reply, "hi, world");
    sys.terminate().await;
}
