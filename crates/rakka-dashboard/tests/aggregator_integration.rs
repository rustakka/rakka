//! End-to-end: spin up two "peer" dashboards and one aggregator node
//! pointed at them; verify `/api/cluster-wide/*` merges results.

#![cfg(feature = "aggregator")]

use axum::body::{to_bytes, Body};
use axum::http::Request;
use tower::ServiceExt;

use rakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use rakka_telemetry::dto::ActorStatus;
use rakka_telemetry::TelemetryExtension;

async fn start_peer(label: &str, actors: &[&str]) -> rakka_dashboard::DashboardHandle {
    let t = TelemetryExtension::new(label, 32);
    for p in actors {
        t.actors.record_spawn(ActorStatus {
            path: (*p).to_string(),
            parent: None,
            actor_type: "Demo".into(),
            mailbox_depth: 0,
            spawned_at: "now".into(),
        });
    }
    t.dead_letters.record("/x".into(), None, "String".into(), "".into());
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        mode: DashboardMode::Local,
        ws_channel_capacity: 16,
        exporters: Default::default(),
    };
    let server = DashboardServer::new(t, cfg);
    server.start().await.unwrap()
}

#[tokio::test]
async fn cluster_wide_overview_sums_peers() {
    let peer_a = start_peer("a", &["/user/a1", "/user/a2"]).await;
    let peer_b = start_peer("b", &["/user/b1"]).await;

    let telemetry = TelemetryExtension::new("agg", 16);
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        mode: DashboardMode::Cluster {
            peers: vec![
                format!("http://{}", peer_a.bound_addr),
                format!("http://{}", peer_b.bound_addr),
            ],
        },
        ws_channel_capacity: 16,
        exporters: Default::default(),
    };
    let router = DashboardServer::new(telemetry, cfg).router();

    let resp = router
        .oneshot(
            Request::get("/api/cluster-wide/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["actor_count"].as_u64().unwrap(), 3);
    assert_eq!(v["dead_letter_count"].as_u64().unwrap(), 2);

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}
