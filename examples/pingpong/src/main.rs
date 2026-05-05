//! PingPong example.

use async_trait::async_trait;
use atomr::prelude::*;

#[derive(Debug)]
enum PingMsg {
    Pong,
    Start(ActorRef<PongMsg>),
}

#[derive(Debug)]
enum PongMsg {
    Ping(ActorRef<PingMsg>),
}

struct Ping {
    count: u32,
    max: u32,
}

#[async_trait]
impl Actor for Ping {
    type Msg = PingMsg;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PingMsg) {
        match msg {
            PingMsg::Start(pong) => {
                println!("ping: starting");
                pong.tell(PongMsg::Ping(_ctx.self_ref().clone()));
            }
            PingMsg::Pong => {
                self.count += 1;
                println!("ping: got pong #{}", self.count);
                if self.count < self.max {
                    // keep bouncing: we don't retain pong ref here for simplicity
                } else {
                    println!("ping: done");
                }
            }
        }
    }
}

struct Pong;

#[async_trait]
impl Actor for Pong {
    type Msg = PongMsg;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PongMsg) {
        match msg {
            PongMsg::Ping(ping) => {
                println!("pong: got ping");
                ping.tell(PingMsg::Pong);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sys = ActorSystem::create("pingpong", Config::empty()).await?;
    let pong = sys.actor_of(Props::create(|| Pong), "pong")?;
    let ping = sys.actor_of(Props::create(|| Ping { count: 0, max: 3 }), "ping")?;
    ping.tell(PingMsg::Start(pong));
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    sys.terminate().await;
    Ok(())
}
