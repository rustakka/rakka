//! `GET /metrics` — Prometheus text-format scrape endpoint. Gated behind
//! the `metrics-prometheus` feature.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::AppState;

pub fn metrics_router(state: AppState) -> Router {
    Router::new().route("/metrics", get(render_metrics)).with_state(state)
}

async fn render_metrics(State(state): State<AppState>) -> Response {
    let Some(exp) = state.exporters.prometheus.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "prometheus exporter not enabled; set exporters.prometheus.enabled=true",
        )
            .into_response();
    };
    exp.seed_from_snapshot(&state.telemetry.snapshot());
    match exp.render() {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}
