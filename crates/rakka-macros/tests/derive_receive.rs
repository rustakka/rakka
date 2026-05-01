//! Phase 1.E — `#[derive(Receive)]` minimal subset (unit variants).

use rakka_config::Config;
use rakka_core::actor::{ActorSystem, Context};
use rakka_macros::Receive;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug)]
enum CounterMsg {
    Inc,
    Reset,
}

#[derive(Receive)]
#[msg(CounterMsg)]
#[receive(unit_variants(Inc, Reset))]
struct Counter {
    n: Arc<Mutex<u32>>,
}

impl Counter {
    async fn on_inc(&mut self, _ctx: &mut Context<Self>) {
        let mut g = self.n.lock().await;
        *g += 1;
    }
    async fn on_reset(&mut self, _ctx: &mut Context<Self>) {
        let mut g = self.n.lock().await;
        *g = 0;
    }
}

#[tokio::test]
async fn derive_receive_dispatches_unit_variants() {
    let sys = ActorSystem::create("DeriveReceive", Config::reference())
        .await
        .unwrap();
    let counter = Arc::new(Mutex::new(0u32));
    let c = counter.clone();
    let r = sys
        .actor_of(
            rakka_core::actor::Props::create(move || Counter { n: c.clone() }),
            "ctr",
        )
        .unwrap();
    r.tell(CounterMsg::Inc);
    r.tell(CounterMsg::Inc);
    r.tell(CounterMsg::Inc);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(*counter.lock().await, 3);
    r.tell(CounterMsg::Reset);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(*counter.lock().await, 0);
    sys.terminate().await;
}
