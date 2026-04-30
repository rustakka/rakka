//! `/api/cluster-wide/*` — aggregated views over peer dashboards. Each
//! handler fans out to the peer list configured via
//! [`crate::DashboardMode::Cluster`] and merges the results. Falls back
//! to the local snapshot when no peers are configured.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};

use rakka_telemetry::dto::{
    ActorSnapshot, ClusterStateInfo, DDataSnapshot, DeadLetterRecord, NodeSnapshot,
    OverviewSnapshot, PersistenceSnapshot, RemoteSnapshot, ShardingSnapshot, StreamsSnapshot,
};

use crate::aggregator::{
    merge_actor_snapshots, merge_cluster_states, merge_overviews, ClusterAggregator,
};
use crate::{AppState, DashboardMode};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/cluster-wide/overview", get(overview))
        .route("/api/cluster-wide/snapshot", get(snapshot))
        .route("/api/cluster-wide/actors", get(actors))
        .route("/api/cluster-wide/dead-letters", get(dead_letters))
        .route("/api/cluster-wide/cluster", get(cluster))
        .route("/api/cluster-wide/sharding", get(sharding))
        .route("/api/cluster-wide/persistence", get(persistence))
        .route("/api/cluster-wide/remote", get(remote))
        .route("/api/cluster-wide/streams", get(streams))
        .route("/api/cluster-wide/ddata", get(ddata))
        .with_state(state)
}

fn aggregator_for(state: &AppState) -> Option<ClusterAggregator> {
    match &state.mode {
        DashboardMode::Cluster { peers } if !peers.is_empty() => {
            Some(ClusterAggregator::new(peers.clone()))
        }
        _ => None,
    }
}

async fn overview(State(state): State<AppState>) -> Json<OverviewSnapshot> {
    if let Some(agg) = aggregator_for(&state) {
        let items = agg.overview_all().await;
        if !items.is_empty() {
            return Json(merge_overviews(&items));
        }
    }
    Json(OverviewSnapshot {
        node: state.telemetry.node.clone(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        actor_count: state.telemetry.actors.live_count(),
        dead_letter_count: state.telemetry.dead_letters.total_count(),
        cluster_member_count: state.telemetry.cluster.member_count(),
        cluster_unreachable_count: state.telemetry.cluster.unreachable_count(),
        remote_association_count: state.telemetry.remote.association_count(),
        running_graphs: state.telemetry.streams.running(),
        persistence_event_count: state.telemetry.persistence.total_events(),
        ddata_key_count: state.telemetry.ddata.key_count(),
    })
}

async fn snapshot(State(state): State<AppState>) -> Json<Vec<NodeSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.snapshots_all().await);
    }
    Json(vec![state.telemetry.snapshot()])
}

async fn actors(State(state): State<AppState>) -> Json<ActorSnapshot> {
    if let Some(agg) = aggregator_for(&state) {
        let items = agg.snapshots_all().await;
        if !items.is_empty() {
            let paired: Vec<(String, ActorSnapshot)> =
                items.into_iter().map(|s| (s.node, s.actors)).collect();
            return Json(merge_actor_snapshots(&paired));
        }
    }
    Json(state.telemetry.actors.snapshot())
}

async fn dead_letters(State(state): State<AppState>) -> Json<Vec<DeadLetterRecord>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.dead_letters_all(100).await);
    }
    Json(state.telemetry.dead_letters.recent(100))
}

async fn cluster(State(state): State<AppState>) -> Json<ClusterStateInfo> {
    if let Some(agg) = aggregator_for(&state) {
        let items = agg.cluster_all().await;
        if !items.is_empty() {
            return Json(merge_cluster_states(&items));
        }
    }
    Json(state.telemetry.cluster.snapshot())
}

async fn sharding(State(state): State<AppState>) -> Json<Vec<ShardingSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.sharding_all().await);
    }
    Json(vec![state.telemetry.sharding.snapshot()])
}

async fn persistence(State(state): State<AppState>) -> Json<Vec<PersistenceSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.persistence_all().await);
    }
    Json(vec![state.telemetry.persistence.snapshot()])
}

async fn remote(State(state): State<AppState>) -> Json<Vec<RemoteSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.remote_all().await);
    }
    Json(vec![state.telemetry.remote.snapshot()])
}

async fn streams(State(state): State<AppState>) -> Json<Vec<StreamsSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.streams_all().await);
    }
    Json(vec![state.telemetry.streams.snapshot()])
}

async fn ddata(State(state): State<AppState>) -> Json<Vec<DDataSnapshot>> {
    if let Some(agg) = aggregator_for(&state) {
        return Json(agg.ddata_all().await);
    }
    Json(vec![state.telemetry.ddata.snapshot()])
}
