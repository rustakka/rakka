//! Streams snapshot.

use axum::extract::State;
use axum::Json;

use atomr_telemetry::dto::StreamsSnapshot;

use crate::AppState;

pub async fn get_streams(State(state): State<AppState>) -> Json<StreamsSnapshot> {
    Json(state.telemetry.streams.snapshot())
}
