//! Cluster state + reachability.

use axum::extract::State;
use axum::Json;

use atomr_telemetry::dto::{ClusterStateInfo, ReachabilityRecord};

use crate::AppState;

pub async fn get_state(State(state): State<AppState>) -> Json<ClusterStateInfo> {
    Json(state.telemetry.cluster.snapshot())
}

pub async fn get_reachability(State(state): State<AppState>) -> Json<Vec<ReachabilityRecord>> {
    Json(state.telemetry.cluster.snapshot().reachability_records)
}
