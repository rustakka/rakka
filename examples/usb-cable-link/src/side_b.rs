//! Side B of a USB-cable-linked actor pair. Sends `Hello` messages to
//! side A's `/user/echo` actor.
//!
//! Usage:
//!   `cargo run --bin cable-side-b -- /dev/ttyACM0 'akka.serial://A@/dev/ttyGS0:0'`
//! Where the second argument is side A's advertised local address as
//! printed by `cable-side-a`.

use std::sync::Arc;
use std::time::Duration;

use atomr_core::actor::ActorSystem;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let device = std::env::args().nth(1).unwrap_or_else(|| "/dev/ttyACM0".into());
    let peer_addr = std::env::args()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("usage: cable-side-b <device> <peer-address>"))?;

    let transport: Arc<dyn Transport> = Arc::new(SerialTransport::new("B", device));
    let sys = ActorSystem::create("B", atomr_config::Config::reference()).await?;
    let remote = RemoteSystem::start_with_transport(sys, transport, RemoteSettings::default()).await?;
    remote.register_bincode::<Hello>();

    let target_path = format!("{peer_addr}/user/echo");
    let echo: ActorRef<Hello> = remote
        .actor_selection::<Hello>(&target_path)
        .ok_or_else(|| anyhow::anyhow!("could not select {target_path}"))?;

    tracing::info!(addr = %remote.local_address, target = %target_path, "side B ready");
    for seq in 0..u32::MAX {
        echo.tell(Hello { text: format!("ping-{seq}"), seq });
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
