//! Remote endpoint snapshot.

use axum::extract::State;
use axum::Json;

use rakka_telemetry::dto::RemoteSnapshot;

use crate::AppState;

pub async fn get_remote(State(state): State<AppState>) -> Json<RemoteSnapshot> {
    Json(state.telemetry.remote.snapshot())
}
