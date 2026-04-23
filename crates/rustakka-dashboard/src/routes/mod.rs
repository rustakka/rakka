//! REST + WebSocket route registration. Each resource gets its own
//! submodule. All handlers share [`crate::AppState`].

use axum::routing::get;
use axum::Router;

use crate::ws::WsHub;
use crate::AppState;

pub mod actors;
pub mod cluster;
pub mod dead_letters;
pub mod ddata;
pub mod overview;
pub mod persistence;
pub mod remote;
pub mod sharding;
pub mod streams;

#[cfg(feature = "metrics-prometheus")]
pub mod metrics;

#[cfg(feature = "aggregator")]
pub mod cluster_wide;

pub fn build_router(state: AppState, ws_capacity: usize) -> Router {
    let hub = WsHub::new(state.telemetry.bus.clone(), ws_capacity);

    let api = Router::new()
        .route("/overview", get(overview::get_overview))
        .route("/actors/tree", get(actors::get_tree))
        .route("/actors", get(actors::list_actors))
        .route("/dead-letters", get(dead_letters::list_dead_letters))
        .route("/cluster/state", get(cluster::get_state))
        .route("/cluster/reachability", get(cluster::get_reachability))
        .route("/sharding", get(sharding::get_sharding))
        .route("/persistence", get(persistence::get_persistence))
        .route("/remote", get(remote::get_remote))
        .route("/streams", get(streams::get_streams))
        .route("/ddata", get(ddata::get_ddata))
        .route("/snapshot", get(overview::get_full_snapshot))
        .with_state(state.clone());

    #[allow(unused_mut)]
    let mut app = Router::new()
        .nest("/api", api)
        .route("/ws", get(crate::ws::ws_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(hub);

    #[cfg(feature = "metrics-prometheus")]
    {
        app = app.merge(metrics::metrics_router(state.clone()));
    }

    #[cfg(feature = "aggregator")]
    {
        app = app.merge(cluster_wide::router(state.clone()));
    }

    #[cfg(feature = "embed-ui")]
    {
        app = app.fallback(crate::spa::serve_embedded);
    }

    app.layer(tower_http::cors::CorsLayer::permissive())
}
