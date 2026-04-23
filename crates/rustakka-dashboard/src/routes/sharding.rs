//! Sharding snapshot.

use axum::extract::State;
use axum::Json;

use rustakka_telemetry::dto::ShardingSnapshot;

use crate::AppState;

pub async fn get_sharding(State(state): State<AppState>) -> Json<ShardingSnapshot> {
    Json(state.telemetry.sharding.snapshot())
}
