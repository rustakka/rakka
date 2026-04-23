// Typed REST client + DTO types mirroring `rustakka-telemetry`'s
// `dto.rs`. Keep this file in sync manually when DTOs change.

export interface ActorStatus {
  path: string;
  parent: string | null;
  actor_type: string;
  mailbox_depth: number;
  spawned_at: string;
}

export interface ActorTreeNode {
  path: string;
  name: string;
  actor_type: string;
  mailbox_depth: number;
  children: ActorTreeNode[];
}

export interface ActorSnapshot {
  total: number;
  roots: ActorTreeNode[];
  flat: ActorStatus[];
}

export interface DeadLetterRecord {
  seq: number;
  recipient: string;
  sender: string | null;
  message_type: string;
  message_preview: string;
  timestamp: string;
}

export interface ClusterMemberInfo {
  address: string;
  status: string;
  roles: string[];
  reachable: boolean;
  up_number: number;
}

export interface ReachabilityRecord {
  observer: string;
  subject: string;
  status: string;
}

export interface ClusterStateInfo {
  self_address: string | null;
  leader: string | null;
  members: ClusterMemberInfo[];
  unreachable: string[];
  reachability_records: ReachabilityRecord[];
  gossip_version: [string, number][];
}

export interface ShardRegionInfo {
  region_id: string;
  shard_count: number;
  shards: string[];
}

export interface ShardingSnapshot {
  regions: ShardRegionInfo[];
  allocations: [string, string][];
}

export interface PersistenceIdStat {
  persistence_id: string;
  highest_sequence_nr: number;
  event_count: number;
}

export interface JournalInfo {
  name: string;
  persistence_ids: PersistenceIdStat[];
}

export interface JournalWriteInfo {
  journal: string;
  persistence_id: string;
  sequence_nr: number;
  timestamp: string;
}

export interface PersistenceSnapshot {
  journals: JournalInfo[];
  total_events: number;
  recent_writes: JournalWriteInfo[];
}

export interface RemoteAssociationInfo {
  remote_address: string;
  state: string;
  inbound_bytes: number;
  outbound_bytes: number;
}

export interface RemoteSnapshot {
  associations: RemoteAssociationInfo[];
}

export interface StreamGraphInfo {
  id: number;
  name: string;
  started_at: string;
}

export interface StreamsSnapshot {
  running_graphs: number;
  total_started: number;
  total_finished: number;
  active: StreamGraphInfo[];
}

export interface DDataSnapshot {
  keys: string[];
  total_updates: number;
}

export interface OverviewSnapshot {
  node: string;
  generated_at: string;
  actor_count: number;
  dead_letter_count: number;
  cluster_member_count: number;
  cluster_unreachable_count: number;
  remote_association_count: number;
  running_graphs: number;
  persistence_event_count: number;
  ddata_key_count: number;
}

export interface NodeSnapshot {
  node: string;
  generated_at: string;
  actors: ActorSnapshot;
  dead_letters: DeadLetterRecord[];
  cluster: ClusterStateInfo;
  sharding: ShardingSnapshot;
  persistence: PersistenceSnapshot;
  remote: RemoteSnapshot;
  streams: StreamsSnapshot;
  ddata: DDataSnapshot;
}

async function get<T>(path: string): Promise<T> {
  const resp = await fetch(path, { credentials: "same-origin" });
  if (!resp.ok) {
    throw new Error(`${resp.status} ${resp.statusText}`);
  }
  return resp.json();
}

export const api = {
  overview: () => get<OverviewSnapshot>("/api/overview"),
  snapshot: () => get<NodeSnapshot>("/api/snapshot"),
  actors: () => get<ActorSnapshot>("/api/actors/tree"),
  deadLetters: (limit = 100) =>
    get<DeadLetterRecord[]>(`/api/dead-letters?limit=${limit}`),
  clusterState: () => get<ClusterStateInfo>("/api/cluster/state"),
  reachability: () => get<ReachabilityRecord[]>("/api/cluster/reachability"),
  sharding: () => get<ShardingSnapshot>("/api/sharding"),
  persistence: () => get<PersistenceSnapshot>("/api/persistence"),
  remote: () => get<RemoteSnapshot>("/api/remote"),
  streams: () => get<StreamsSnapshot>("/api/streams"),
  ddata: () => get<DDataSnapshot>("/api/ddata"),
};
