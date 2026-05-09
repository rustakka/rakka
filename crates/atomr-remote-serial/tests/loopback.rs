//! Two `SerialTransport`s back-to-back over a `tokio::io::duplex`
//! pair. Verifies that AkkaPdu frames round-trip through the
//! `read_frame`/`write_frame` codec and that `from:` attribution is
//! taken from the peer's `Associate` PDU.

use std::sync::Arc;
use std::time::Duration;

use atomr_core::actor::Address;
use atomr_remote::pdu::{AkkaPdu, AssociateInfo, DisassociateReason, PROTOCOL_VERSION};
use atomr_remote::transport::Transport;
use atomr_remote_serial::SerialTransport;

const MAX_FRAME: usize = 64 * 1024;

fn associate_pdu(origin: Address, uid: u64) -> AkkaPdu {
    AkkaPdu::Associate(AssociateInfo { origin, uid, cookie: None, protocol_version: PROTOCOL_VERSION })
}

#[tokio::test]
async fn pdu_roundtrip_over_duplex() {
    let (a_io, b_io) = tokio::io::duplex(8192);
    let (a_reader, a_writer) = tokio::io::split(a_io);
    let (b_reader, b_writer) = tokio::io::split(b_io);

    let a = Arc::new(SerialTransport::with_streams("A", a_reader, a_writer, MAX_FRAME));
    let b = Arc::new(SerialTransport::with_streams("B", b_reader, b_writer, MAX_FRAME));

    let addr_a = a.local_address().expect("local address");
    let addr_b = b.local_address().expect("local address");
    let mut inbound_a = a.inbound();
    let mut inbound_b = b.inbound();

    // Each side sends an Associate first — that's the protocol layer's
    // job in production, but here we drive the bytes ourselves.
    a.send(&addr_b, associate_pdu(addr_a.clone(), 7)).await.unwrap();
    b.send(&addr_a, associate_pdu(addr_b.clone(), 11)).await.unwrap();

    let frame_b = tokio::time::timeout(Duration::from_millis(500), inbound_b.recv())
        .await
        .expect("timeout")
        .expect("inbound closed");
    match frame_b.pdu {
        AkkaPdu::Associate(info) => {
            assert_eq!(info.origin, addr_a);
            assert_eq!(info.uid, 7);
            assert_eq!(frame_b.from, addr_a, "B attributes incoming frames to A's advertised address");
        }
        other => panic!("unexpected pdu on B: {other:?}"),
    }

    let frame_a = tokio::time::timeout(Duration::from_millis(500), inbound_a.recv())
        .await
        .expect("timeout")
        .expect("inbound closed");
    match frame_a.pdu {
        AkkaPdu::Associate(info) => {
            assert_eq!(info.origin, addr_b);
            assert_eq!(info.uid, 11);
            assert_eq!(frame_a.from, addr_b);
        }
        other => panic!("unexpected pdu on A: {other:?}"),
    }

    // Now drive heartbeat + disassociate.
    a.send(&addr_b, AkkaPdu::Heartbeat).await.unwrap();
    let hb = tokio::time::timeout(Duration::from_millis(500), inbound_b.recv())
        .await
        .expect("timeout")
        .expect("inbound closed");
    assert!(matches!(hb.pdu, AkkaPdu::Heartbeat));
    assert_eq!(hb.from, addr_a);

    a.disassociate(&addr_b).await.unwrap();
    let dis = tokio::time::timeout(Duration::from_millis(500), inbound_b.recv())
        .await
        .expect("timeout")
        .expect("inbound closed");
    assert!(matches!(dis.pdu, AkkaPdu::Disassociate(DisassociateReason::Normal)));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn unassociated_send_returns_closed() {
    use atomr_remote::transport::TransportError;

    // Build a transport via with_streams but immediately drop the
    // *peer's* writer half — the link is up but the runner has no peer.
    // Actually: with_streams runs the link runner even with no peer
    // bytes, so the sender is set. To exercise the "closed" path we
    // shutdown first.
    let (a_io, _b_io) = tokio::io::duplex(8192);
    let (a_reader, a_writer) = tokio::io::split(a_io);
    let a = SerialTransport::with_streams("A", a_reader, a_writer, MAX_FRAME);
    a.shutdown().await.unwrap();

    let result = a.send(&Address::remote("akka.serial", "B", "/dev/null", 0), AkkaPdu::Heartbeat).await;
    assert!(matches!(result, Err(TransportError::Closed)));
}
