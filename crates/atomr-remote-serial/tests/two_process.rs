//! End-to-end remoting over `SerialTransport` using two cross-wired
//! duplex pipes as a stand-in for a USB cable. Mirrors
//! `atomr-remote/tests/two_process.rs` but with no TCP and no IP
//! addressing — just bytes on a stream.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

#[tokio::test]
async fn tell_crosses_serial_cable() {
    // The "cable" is a pair of duplex pipes — bytes A writes show up
    // at B's reader, and vice versa.
    let (a_io, b_io) = tokio::io::duplex(64 * 1024);
    let (a_reader, a_writer) = tokio::io::split(a_io);
    let (b_reader, b_writer) = tokio::io::split(b_io);

    let transport_a: Arc<dyn Transport> = Arc::new(SerialTransport::with_streams(
        "A",
        a_reader,
        a_writer,
        4 * 1024 * 1024,
    ));
    let transport_b: Arc<dyn Transport> = Arc::new(SerialTransport::with_streams(
        "B",
        b_reader,
        b_writer,
        4 * 1024 * 1024,
    ));

    let sys_a = ActorSystem::create("A", atomr_config::Config::reference()).await.unwrap();
    let sys_b = ActorSystem::create("B", atomr_config::Config::reference()).await.unwrap();

    let a = RemoteSystem::start_with_transport(sys_a, transport_a, RemoteSettings::default())
        .await
        .unwrap();
    let b = RemoteSystem::start_with_transport(sys_b, transport_b, RemoteSettings::default())
        .await
        .unwrap();

    a.register_bincode::<Hello>();
    b.register_bincode::<Hello>();

    let received = Arc::new(AtomicU32::new(0));
    let last = Arc::new(parking_lot::Mutex::new(None));
    let r1 = received.clone();
    let l1 = last.clone();
    let echo = a
        .system
        .actor_of(
            Props::create(move || Recorder { received: r1.clone(), last_text: l1.clone() }),
            "echo",
        )
        .unwrap();
    a.expose_actor(echo);

    // From B, look up A's `echo` and tell.
    let target_path = format!("{}/user/echo", a.local_address);
    let remote: ActorRef<Hello> =
        b.actor_selection::<Hello>(&target_path).expect("remote selection");
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
    assert_eq!(received.load(Ordering::SeqCst), 3, "expected 3 inbound messages");
    assert!(last.lock().as_deref().unwrap_or("").starts_with("hi-"));

    a.shutdown().await;
    b.shutdown().await;
}
