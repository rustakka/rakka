//! Persistence snapshot.

use axum::extract::State;
use axum::Json;

use atomr_telemetry::dto::PersistenceSnapshot;

use crate::AppState;

pub async fn get_persistence(State(state): State<AppState>) -> Json<PersistenceSnapshot> {
    Json(state.telemetry.persistence.snapshot_async().await)
}
