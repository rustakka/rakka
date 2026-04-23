//! Dead-letter listing with optional `?limit=` parameter.

use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;

use rustakka_telemetry::dto::DeadLetterRecord;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct DeadLetterQuery {
    pub limit: Option<usize>,
}

pub async fn list_dead_letters(
    State(state): State<AppState>,
    Query(q): Query<DeadLetterQuery>,
) -> Json<Vec<DeadLetterRecord>> {
    Json(state.telemetry.dead_letters.recent(q.limit.unwrap_or(100)))
}
