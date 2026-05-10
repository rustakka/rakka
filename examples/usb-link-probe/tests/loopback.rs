//! In-memory roundtrip of the link-probe protocol over a
//! `tokio::io::duplex` pair. No real serial device involved.
//!
//! Mirrors the pattern in `crates/atomr-remote-serial/tests/loopback.rs`
//! but at the actor / RemoteSystem layer rather than the raw PDU layer.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::timeout;

use atomr_core::actor::{ActorSystem, Props};
use atomr_remote::transport::Transport;
use atomr_remote::{RemoteSettings, RemoteSystem};
use atomr_remote_serial::SerialTransport;

// Pull the example's own peer + stats modules in. `cargo test` builds
// the bin's library tree, so a `#[path = ...]` include works for files
// rooted under `src/`.
#[path = "../src/peer.rs"]
mod peer;
#[path = "../src/stats.rs"]
mod stats;

use peer::{LinkMsg, Peer};
use stats::Stats;

const MAX_FRAME: usize = 64 * 1024;

/// End-to-end round-trip: B selects A's `/user/peer`, sends a
/// `LinkMsg::Chat`, and we verify A's inbound channel receives it.
/// Then we send a `Ping` from B → A, expect A to forward it through
/// its inbound channel, and exercise the stats recorder by hand.
#[tokio::test]
async fn chat_and_ping_roundtrip_over_duplex() -> Result<()> {
    let (a_io, b_io) = tokio::io::duplex(8192);
    let (a_reader, a_writer) = tokio::io::split(a_io);
    let (b_reader, b_writer) = tokio::io::split(b_io);

    let transport_a: Arc<dyn Transport> =
        Arc::new(SerialTransport::with_streams("A", a_reader, a_writer, MAX_FRAME));
    let transport_b: Arc<dyn Transport> =
        Arc::new(SerialTransport::with_streams("B", b_reader, b_writer, MAX_FRAME));

    let sys_a = ActorSystem::create("A", atomr_config::Config::reference()).await?;
    let sys_b = ActorSystem::create("B", atomr_config::Config::reference()).await?;
    let remote_a = RemoteSystem::start_with_transport(sys_a, transport_a, RemoteSettings::default()).await?;
    let remote_b = RemoteSystem::start_with_transport(sys_b, transport_b, RemoteSettings::default()).await?;

    remote_a.register_bincode::<LinkMsg>();
    remote_b.register_bincode::<LinkMsg>();

    let (tx_a, mut rx_a) = unbounded_channel::<LinkMsg>();
    let peer_a = remote_a
        .system
        .actor_of(Props::create(move || Peer::new(tx_a.clone())), "peer")
        .map_err(|e| anyhow::anyhow!("spawn peer on A: {e:?}"))?;
    remote_a.expose_actor(peer_a);

    // B reaches into A.
    let target_path = format!("{}/user/peer", remote_a.local_address);
    let target = remote_b
        .actor_selection::<LinkMsg>(&target_path)
        .ok_or_else(|| anyhow::anyhow!("actor_selection({target_path}) returned None"))?;

    target.tell(LinkMsg::Chat { body: "hello over usb".into() });
    let received = timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for Chat on A"))?
        .ok_or_else(|| anyhow::anyhow!("inbound closed before Chat arrived"))?;
    match received {
        LinkMsg::Chat { body } => assert_eq!(body, "hello over usb"),
        other => panic!("expected Chat, got {other:?}"),
    }

    target.tell(LinkMsg::Ping {
        seq: 42,
        sent_at_micros: 1_000_000,
        from_addr: remote_b.local_address.to_string(),
    });
    let received = timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for Ping on A"))?
        .ok_or_else(|| anyhow::anyhow!("inbound closed before Ping arrived"))?;
    match received {
        LinkMsg::Ping { seq, sent_at_micros, from_addr } => {
            assert_eq!(seq, 42);
            assert_eq!(sent_at_micros, 1_000_000);
            assert_eq!(from_addr, remote_b.local_address.to_string());
        }
        other => panic!("expected Ping, got {other:?}"),
    }

    // Stats recorder is exercised independently of the wire — give it
    // some samples and verify the snapshot sums correctly. Ensures the
    // shared module compiles into the test binary.
    let stats = Stats::new();
    stats.record_sent();
    stats.record_recv(Duration::from_millis(3));
    let snap = stats.snapshot();
    assert_eq!(snap.sent, 1);
    assert_eq!(snap.recvd, 1);
    assert_eq!(snap.p50, Some(Duration::from_millis(3)));

    remote_a.shutdown().await;
    remote_b.shutdown().await;
    Ok(())
}
