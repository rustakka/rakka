use rakka_core::prelude::*;
use rakka_macros::{actor_msg, Actor};

#[actor_msg]
#[allow(dead_code)]
enum CounterMsg {
    Inc,
    Get(tokio::sync::oneshot::Sender<u64>),
}

#[derive(Default, Actor)]
#[msg(CounterMsg)]
struct Counter {
    value: u64,
}

impl Counter {
    async fn handle_msg(&mut self, _ctx: &mut Context<Self>, msg: CounterMsg) {
        match msg {
            CounterMsg::Inc => self.value += 1,
            CounterMsg::Get(reply) => {
                let _ = reply.send(self.value);
            }
        }
    }
}

#[tokio::test]
async fn derived_actor_roundtrip() {
    let sys = ActorSystem::create("macro-test", Config::empty()).await.unwrap();
    let r = sys.actor_of(Props::create(Counter::default), "c").unwrap();
    r.tell(CounterMsg::Inc);
    r.tell(CounterMsg::Inc);
    r.tell(CounterMsg::Inc);
    let (tx, rx) = tokio::sync::oneshot::channel();
    r.tell(CounterMsg::Get(tx));
    let v = rx.await.unwrap();
    assert_eq!(v, 3);
    sys.terminate().await;
}
