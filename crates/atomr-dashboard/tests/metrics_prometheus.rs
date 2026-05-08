//! Scrape `/metrics` and verify the Prometheus exposition body contains
//! counters/gauges populated by the telemetry probes. Gated behind the
//! `metrics-prometheus` feature so CI without the feature still compiles.

#![cfg(feature = "metrics-prometheus")]

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use atomr_telemetry::dto::{ActorStatus, DeadLetterRecord};
use atomr_telemetry::exporters::config::{ExportersConfig, PrometheusConfig};
use atomr_telemetry::TelemetryExtension;

use atomr_dashboard::{DashboardConfig, DashboardMode, DashboardServer};

#[tokio::test]
async fn metrics_endpoint_renders_registry() {
    let telemetry = TelemetryExtension::new("node-a", 64);
    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/a".into(),
        parent: Some("/user".into()),
        actor_type: "Demo".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
        host: None,
    });
    telemetry.dead_letters.record("/user/a".into(), None, "String".into(), "hi".into());
    let _ = DeadLetterRecord {
        seq: 0,
        recipient: "".into(),
        sender: None,
        message_type: "".into(),
        message_preview: "".into(),
        timestamp: "".into(),
    };

    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        mode: DashboardMode::Local,
        ws_channel_capacity: 32,
        exporters: ExportersConfig {
            prometheus: Some(PrometheusConfig { namespace: Some("atomr".into()), enabled: true }),
            otlp: None,
        },
    };
    let server = DashboardServer::new(telemetry.clone(), cfg);
    let app = server.router_with_exporters().expect("apply exporters");

    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/b".into(),
        parent: Some("/user".into()),
        actor_type: "Demo".into(),
        mailbox_depth: 3,
        spawned_at: "now".into(),
        host: None,
    });
    telemetry.dead_letters.record("/user/b".into(), None, "String".into(), "dead".into());

    let resp = app.oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 128 * 1024).await.unwrap();
    let body = String::from_utf8(bytes.to_vec()).unwrap();

    assert!(body.contains("atomr_actors_spawned_total"), "body: {body}");
    assert!(body.contains("atomr_dead_letters_total"), "body: {body}");
    assert!(body.contains("node=\"node-a\""), "body: {body}");
}
