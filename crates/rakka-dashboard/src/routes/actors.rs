//! Actor tree + flat listing.

use axum::extract::State;
use axum::Json;

use rakka_telemetry::dto::ActorSnapshot;

use crate::AppState;

pub async fn get_tree(State(state): State<AppState>) -> Json<ActorSnapshot> {
    Json(state.telemetry.actors.snapshot())
}

pub async fn list_actors(State(state): State<AppState>) -> Json<ActorSnapshot> {
    Json(state.telemetry.actors.snapshot())
}
