//! Integration test for the `/ws` multiplexer: binds a real server,
//! connects as a client, publishes events, and verifies topic filters.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use atomr_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use atomr_telemetry::dto::ActorStatus;
use atomr_telemetry::TelemetryExtension;

async fn start_server() -> (atomr_dashboard::DashboardHandle, std::sync::Arc<TelemetryExtension>) {
    let telemetry = TelemetryExtension::new("ws-node", 32);
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        mode: DashboardMode::Local,
        ws_channel_capacity: 32,
        exporters: Default::default(),
    };
    let server = DashboardServer::new(telemetry.clone(), cfg);
    let handle = server.start().await.unwrap();
    (handle, telemetry)
}

async fn read_text(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> Option<String> {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), socket.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Some(t),
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = socket.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

#[tokio::test]
async fn ws_forwards_actor_events_without_filter() {
    let (handle, telemetry) = start_server().await;
    let url = format!("ws://{}/ws", handle.bound_addr);
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/ws".into(),
        parent: Some("/user".into()),
        actor_type: "Demo".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
    });

    let msg = read_text(&mut socket).await.expect("text frame");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["kind"], "actor_spawned");
    handle.shutdown().await;
}

#[tokio::test]
async fn ws_topic_filter_excludes_non_matching_events() {
    let (handle, telemetry) = start_server().await;
    let url = format!("ws://{}/ws?topics=dead_letters", handle.bound_addr);
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/ignored".into(),
        parent: None,
        actor_type: "Demo".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
    });
    telemetry.dead_letters.record("/user/x".into(), None, "String".into(), "".into());

    let msg = read_text(&mut socket).await.expect("text frame");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["kind"], "dead_letter");
    handle.shutdown().await;
}
