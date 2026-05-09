//! Side A of a USB-cable-linked actor pair. Hosts an `Echo` actor and
//! runs forever; expects side B to send `Hello` messages.
//!
//! Usage:
//!   `cargo run --bin cable-side-a -- /dev/ttyACM0`
//! (or `/dev/ttyGS0` if you're running this on the gadget side).

use std::sync::Arc;

use atomr_core::actor::{ActorSystem, Props};
use atomr_core::prelude::*;
use atomr_remote::transport::Transport;
use atomr_remote::{RemoteSettings, RemoteSystem};
use atomr_remote_serial::SerialTransport;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hello {
    text: String,
    seq: u32,
}

struct Echo;

#[async_trait]
impl Actor for Echo {
    type Msg = Hello;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Hello) {
        tracing::info!(seq = msg.seq, text = %msg.text, "received");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let device = std::env::args().nth(1).unwrap_or_else(|| "/dev/ttyACM0".into());

    let transport: Arc<dyn Transport> = Arc::new(SerialTransport::new("A", device));
    let sys = ActorSystem::create("A", atomr_config::Config::reference()).await?;
    let remote = RemoteSystem::start_with_transport(sys, transport, RemoteSettings::default()).await?;
    remote.register_bincode::<Hello>();

    let echo = remote.system.actor_of(Props::create(|| Echo), "echo")?;
    remote.expose_actor(echo);

    tracing::info!(addr = %remote.local_address, "side A ready, expose actor at /user/echo");
    tokio::signal::ctrl_c().await?;
    remote.shutdown().await;
    Ok(())
}
