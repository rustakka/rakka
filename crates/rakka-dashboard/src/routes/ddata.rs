//! Distributed-data snapshot.

use axum::extract::State;
use axum::Json;

use rakka_telemetry::dto::DDataSnapshot;

use crate::AppState;

pub async fn get_ddata(State(state): State<AppState>) -> Json<DDataSnapshot> {
    Json(state.telemetry.ddata.snapshot())
}
