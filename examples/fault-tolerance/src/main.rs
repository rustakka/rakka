//! Fault-tolerance example. akka.net: `src/examples/FaultTolerance`.
//!
//! Demonstrates supervision with OneForOne strategy and restart semantics.

use async_trait::async_trait;
use atomr::prelude::*;

#[derive(Debug)]
enum Cmd {
    Work(i32),
    Boom,
}

struct Worker {
    processed: u32,
}

#[async_trait]
impl Actor for Worker {
    type Msg = Cmd;

    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {
        println!("worker: pre_start");
    }

    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {
        println!("worker: post_stop");
    }

    async fn post_restart(&mut self, _ctx: &mut Context<Self>, err: &str) {
        println!("worker: post_restart (err = {err})");
    }

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Cmd) {
        match msg {
            Cmd::Work(n) => {
                self.processed += 1;
                println!("worker: processed {n} (total {})", self.processed);
            }
            Cmd::Boom => panic!("boom!"),
        }
    }

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        SupervisorStrategy::default()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sys = ActorSystem::create("fault", Config::empty()).await?;
    let worker = sys.actor_of(Props::create(|| Worker { processed: 0 }), "worker")?;
    worker.tell(Cmd::Work(1));
    worker.tell(Cmd::Work(2));
    worker.tell(Cmd::Boom);
    worker.tell(Cmd::Work(3));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    sys.terminate().await;
    Ok(())
}
