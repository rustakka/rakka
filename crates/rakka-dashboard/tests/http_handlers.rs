//! Handler-level tests exercising the REST surface via
//! `tower::ServiceExt::oneshot` against an in-memory telemetry state.
//! No sockets are bound.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use rakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use rakka_telemetry::dto::ActorStatus;
use rakka_telemetry::TelemetryExtension;

fn make_server() -> (Arc<TelemetryExtension>, DashboardServer) {
    let telemetry = TelemetryExtension::new("test-node", 64);
    telemetry.actors.record_spawn(ActorStatus {
        path: "/user/a".into(),
        parent: Some("/user".into()),
        actor_type: "Demo".into(),
        mailbox_depth: 0,
        spawned_at: "now".into(),
    });
    telemetry.dead_letters.record("/user/a".into(), None, "String".into(), "hi".into());
    let cfg = DashboardConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        mode: DashboardMode::Local,
        ws_channel_capacity: 32,
        exporters: Default::default(),
    };
    let server = DashboardServer::new(telemetry.clone(), cfg);
    (telemetry, server)
}

async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn overview_returns_vitals() {
    let (_t, server) = make_server();
    let app = server.router();
    let resp = app
        .oneshot(Request::get("/api/overview").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["node"], "test-node");
    assert_eq!(body["actor_count"], 1);
    assert_eq!(body["dead_letter_count"], 1);
}

#[tokio::test]
async fn snapshot_is_full_payload() {
    let (_t, server) = make_server();
    let app = server.router();
    let resp = app
        .oneshot(Request::get("/api/snapshot").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["actors"]["total"].as_u64().unwrap() >= 1);
    assert!(body["dead_letters"].is_array());
    assert!(body["cluster"].is_object());
}

#[tokio::test]
async fn dead_letters_list_with_limit() {
    let (_t, server) = make_server();
    let app = server.router();
    let resp = app
        .oneshot(Request::get("/api/dead-letters?limit=10").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn actors_tree_returns_roots() {
    let (_t, server) = make_server();
    let app = server.router();
    let resp = app
        .oneshot(Request::get("/api/actors/tree").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["roots"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
async fn healthz_is_ok() {
    let (_t, server) = make_server();
    let app = server.router();
    let resp = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn cluster_sharding_remote_streams_ddata_respond_ok() {
    let (_t, server) = make_server();
    let app = server.router();
    for path in [
        "/api/cluster/state",
        "/api/cluster/reachability",
        "/api/sharding",
        "/api/persistence",
        "/api/remote",
        "/api/streams",
        "/api/ddata",
    ] {
        let resp = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path} should be 200");
    }
}
