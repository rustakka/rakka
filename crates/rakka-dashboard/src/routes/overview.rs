//! `/api/overview` + `/api/snapshot` — roll-up vitals and full JSON
//! payload.

use axum::extract::State;
use axum::Json;

use rakka_telemetry::dto::{NodeSnapshot, OverviewSnapshot};

use crate::AppState;

pub async fn get_overview(State(state): State<AppState>) -> Json<OverviewSnapshot> {
    let t = &state.telemetry;
    Json(OverviewSnapshot {
        node: t.node.clone(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        actor_count: t.actors.live_count(),
        dead_letter_count: t.dead_letters.total_count(),
        cluster_member_count: t.cluster.member_count(),
        cluster_unreachable_count: t.cluster.unreachable_count(),
        remote_association_count: t.remote.association_count(),
        running_graphs: t.streams.running(),
        persistence_event_count: t.persistence.total_events(),
        ddata_key_count: t.ddata.key_count(),
    })
}

pub async fn get_full_snapshot(State(state): State<AppState>) -> Json<NodeSnapshot> {
    Json(state.telemetry.snapshot())
}
