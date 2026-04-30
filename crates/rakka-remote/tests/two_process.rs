//! End-to-end remoting tests. Two `ActorSystem`s on distinct localhost
//! ports exchange messages over the real `TcpTransport` + Akka protocol
//! handshake.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::actor::{ActorSystem, Address, Props};
use rakka_core::prelude::*;
use rakka_remote::{RemoteSettings, RemoteSystem};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Hello {
    text: String,
    seq: u32,
}

struct Recorder {
    received: Arc<AtomicU32>,
    last_text: Arc<parking_lot::Mutex<Option<String>>>,
}

#[async_trait]
impl Actor for Recorder {
    type Msg = Hello;
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Hello) {
        self.received.fetch_add(1, Ordering::SeqCst);
        *self.last_text.lock() = Some(msg.text);
    }
}

async fn boot(name: &str) -> RemoteSystem {
    let sys = ActorSystem::create(name, rakka_config::Config::reference())
        .await
        .unwrap();
    RemoteSystem::start(sys, "127.0.0.1:0".parse().unwrap(), RemoteSettings::default())
        .await
        .unwrap()
}

fn spawn_recorder(
    sys: &ActorSystem,
    name: &str,
) -> (
    rakka_core::actor::ActorRef<Hello>,
    Arc<AtomicU32>,
    Arc<parking_lot::Mutex<Option<String>>>,
) {
    let received = Arc::new(AtomicU32::new(0));
    let last = Arc::new(parking_lot::Mutex::new(None));
    let r1 = received.clone();
    let l1 = last.clone();
    let r = sys
        .actor_of(
            Props::create(move || Recorder { received: r1.clone(), last_text: l1.clone() }),
            name,
        )
        .unwrap();
    (r, received, last)
}

#[tokio::test]
async fn tell_crosses_process_boundary() {
    let a = boot("A").await;
    let b = boot("B").await;
    a.register_bincode::<Hello>();
    b.register_bincode::<Hello>();

    let (echo, received, last) = spawn_recorder(&a.system, "echo");
    a.expose_actor(echo);

    // From B, look up A's `echo` and send a Hello.
    let target_path = format!("{}/user/echo", a.local_address);
    let remote: ActorRef<Hello> = b
        .actor_selection::<Hello>(&target_path)
        .expect("remote selection");
    for i in 0..3 {
        remote.tell(Hello { text: format!("hi-{i}"), seq: i });
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if received.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        received.load(Ordering::SeqCst),
        3,
        "expected 3 inbound messages",
    );
    assert!(last.lock().as_deref().unwrap_or("").starts_with("hi-"));

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn endpoint_manager_tracks_peer_state() {
    let a = boot("A").await;
    let b = boot("B").await;
    a.register_bincode::<u32>();
    b.register_bincode::<u32>();

    let _ = a.endpoint_manager().endpoint_for(&b.local_address).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let states = a.endpoint_manager().peer_states();
    assert!(
        states.iter().any(|(addr, state, _)| addr == &b.local_address.to_string()
            && (*state == "connected" || *state == "pending")),
        "expected to see B in peer states, got {states:?}"
    );

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn metrics_record_sent_messages() {
    let a = boot("A").await;
    let b = boot("B").await;
    a.register_bincode::<Hello>();
    b.register_bincode::<Hello>();

    let (echo, received, _last) = spawn_recorder(&b.system, "echo");
    b.expose_actor(echo);

    let target = format!("{}/user/echo", b.local_address);
    let remote: ActorRef<Hello> =
        a.actor_selection::<Hello>(&target).expect("remote selection");
    for i in 0..5 {
        remote.tell(Hello { text: format!("m{i}"), seq: i });
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && received.load(Ordering::SeqCst) < 5 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let snap = a.endpoint_manager().metrics().snapshot();
    let to_b: Vec<_> = snap
        .per_address
        .iter()
        .filter(|r| r.address == b.local_address.to_string())
        .collect();
    assert!(!to_b.is_empty(), "expected metrics for {}", b.local_address);
    let row = to_b[0];
    assert!(row.sent_messages >= 5, "sent_messages = {}", row.sent_messages);

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn unknown_codec_drops_silently() {
    // Sender registers Hello, receiver does not — the receiver should
    // log + drop, not panic, and not deliver anything.
    let a = boot("A").await;
    let b = boot("B").await;
    a.register_bincode::<Hello>();

    let (echo, received, _last) = spawn_recorder(&b.system, "echo");
    b.expose_actor(echo);

    let target = format!("{}/user/echo", b.local_address);
    let remote: ActorRef<Hello> =
        a.actor_selection::<Hello>(&target).expect("remote selection");
    remote.tell(Hello { text: "ignored".into(), seq: 0 });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(received.load(Ordering::SeqCst), 0);

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn handshake_protocol_version_carried_on_wire() {
    use rakka_remote::pdu::{AkkaPdu, AssociateInfo};
    use rakka_remote::transport::{TcpTransport, Transport};

    let t = TcpTransport::new("Compat", "127.0.0.1:0".parse().unwrap());
    let bound = t.listen().await.unwrap();
    let port = bound.port.unwrap();

    let peer = TcpTransport::new("Bad", "127.0.0.1:0".parse().unwrap());
    let _ = peer.listen().await.unwrap();
    let target = Address::remote("akka.tcp", "Compat", "127.0.0.1", port);
    peer.associate(&target).await.unwrap();
    peer.send(
        &target,
        AkkaPdu::Associate(AssociateInfo {
            origin: Address::remote("akka.tcp", "Bad", "127.0.0.1", 1),
            uid: 9,
            cookie: None,
            protocol_version: 999,
        }),
    )
    .await
    .unwrap();

    let mut inbound = t.inbound();
    let frame = tokio::time::timeout(Duration::from_millis(500), inbound.recv())
        .await
        .unwrap()
        .unwrap();
    match frame.pdu {
        AkkaPdu::Associate(info) => assert_eq!(info.protocol_version, 999),
        other => panic!("expected Associate, got {other:?}"),
    }

    t.shutdown().await.unwrap();
    peer.shutdown().await.unwrap();
}
