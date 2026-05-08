//! Serde DTOs shared between the telemetry bus, REST handlers, and the
//! React dashboard. Kept in one file so the whole wire format is visible
//! at a glance.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorStatus {
    pub path: String,
    pub parent: Option<String>,
    pub actor_type: String,
    pub mailbox_depth: u64,
    pub spawned_at: String,
    /// Optional host annotation supplied by the application via
    /// [`crate::actor_registry::ActorRegistry::set_host`]. Single-process
    /// systems leave this `None`; cluster-aware demos use it to group
    /// actors by their owning cluster member on the dashboard's
    /// Topology page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorTreeNode {
    pub path: String,
    pub name: String,
    pub actor_type: String,
    pub mailbox_depth: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub children: Vec<ActorTreeNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActorSnapshot {
    pub total: u64,
    pub roots: Vec<ActorTreeNode>,
    pub flat: Vec<ActorStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub seq: u64,
    pub recipient: String,
    pub sender: Option<String>,
    pub message_type: String,
    pub message_preview: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterMemberInfo {
    pub address: String,
    pub status: String,
    pub roles: Vec<String>,
    pub reachable: bool,
    pub up_number: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterStateInfo {
    pub self_address: Option<String>,
    pub leader: Option<String>,
    pub members: Vec<ClusterMemberInfo>,
    pub unreachable: Vec<String>,
    pub reachability_records: Vec<ReachabilityRecord>,
    pub gossip_version: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReachabilityRecord {
    pub observer: String,
    pub subject: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMembershipDiff {
    pub added: Vec<ClusterMemberInfo>,
    pub updated: Vec<ClusterMemberInfo>,
    pub removed: Vec<String>,
    pub became_unreachable: Vec<String>,
    pub became_reachable: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardRegionInfo {
    pub region_id: String,
    pub shard_count: usize,
    pub shards: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardingSnapshot {
    pub regions: Vec<ShardRegionInfo>,
    pub allocations: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingEvent {
    pub region_id: String,
    pub shard_id: String,
    pub event: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistenceSnapshot {
    pub journals: Vec<JournalInfo>,
    pub total_events: u64,
    pub recent_writes: Vec<JournalWriteInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JournalInfo {
    pub name: String,
    pub persistence_ids: Vec<PersistenceIdStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceIdStat {
    pub persistence_id: String,
    pub highest_sequence_nr: u64,
    pub event_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalWriteInfo {
    pub journal: String,
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RemoteSnapshot {
    pub associations: Vec<RemoteAssociationInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteAssociationInfo {
    pub remote_address: String,
    pub state: String,
    pub inbound_bytes: u64,
    pub outbound_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamsSnapshot {
    pub running_graphs: u64,
    pub total_started: u64,
    pub total_finished: u64,
    pub active: Vec<StreamGraphInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamGraphInfo {
    pub id: u64,
    pub name: String,
    pub started_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DDataSnapshot {
    pub keys: Vec<String>,
    pub total_updates: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub node: String,
    pub generated_at: String,
    pub actors: ActorSnapshot,
    pub dead_letters: Vec<DeadLetterRecord>,
    pub cluster: ClusterStateInfo,
    pub sharding: ShardingSnapshot,
    pub persistence: PersistenceSnapshot,
    pub remote: RemoteSnapshot,
    pub streams: StreamsSnapshot,
    pub ddata: DDataSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewSnapshot {
    pub node: String,
    pub generated_at: String,
    pub actor_count: u64,
    pub dead_letter_count: u64,
    pub cluster_member_count: usize,
    pub cluster_unreachable_count: usize,
    pub remote_association_count: usize,
    pub running_graphs: u64,
    pub persistence_event_count: u64,
    pub ddata_key_count: usize,
}
