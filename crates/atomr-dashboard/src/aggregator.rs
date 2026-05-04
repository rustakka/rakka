//! Cluster-mode aggregator — fans telemetry requests out to peer
//! dashboards and merges their responses into a unified view.
//!
//! Gated behind the `aggregator` cargo feature so that single-node
//! deployments don't pull in `reqwest`.

#[cfg(feature = "aggregator")]
mod imp {
    use atomr_telemetry::dto::{
        ActorSnapshot, ActorStatus, ClusterMemberInfo, ClusterStateInfo, DDataSnapshot, DeadLetterRecord,
        NodeSnapshot, OverviewSnapshot, PersistenceSnapshot, RemoteSnapshot, ShardingSnapshot,
        StreamsSnapshot,
    };

    #[derive(Clone, Debug)]
    pub struct ClusterAggregator {
        pub peers: Vec<String>,
        client: reqwest::Client,
    }

    impl ClusterAggregator {
        pub fn new(peers: Vec<String>) -> Self {
            Self { peers, client: reqwest::Client::new() }
        }

        async fn fetch<T: serde::de::DeserializeOwned>(&self, path: &str) -> Vec<T> {
            let mut out = Vec::new();
            for peer in &self.peers {
                let url = format!("{}{path}", peer.trim_end_matches('/'));
                if let Ok(resp) = self.client.get(&url).send().await {
                    if let Ok(t) = resp.json::<T>().await {
                        out.push(t);
                    }
                }
            }
            out
        }

        pub async fn overview_all(&self) -> Vec<OverviewSnapshot> {
            self.fetch("/api/overview").await
        }

        pub async fn snapshots_all(&self) -> Vec<NodeSnapshot> {
            self.fetch("/api/snapshot").await
        }

        pub async fn dead_letters_all(&self, limit: usize) -> Vec<DeadLetterRecord> {
            let path = format!("/api/dead-letters?limit={limit}");
            let mut out: Vec<DeadLetterRecord> = Vec::new();
            for peer in &self.peers {
                let url = format!("{}{path}", peer.trim_end_matches('/'));
                if let Ok(resp) = self.client.get(&url).send().await {
                    if let Ok(mut list) = resp.json::<Vec<DeadLetterRecord>>().await {
                        out.append(&mut list);
                    }
                }
            }
            out
        }

        pub async fn actors_all(&self) -> Vec<ActorSnapshot> {
            self.fetch("/api/actors/tree").await
        }

        pub async fn cluster_all(&self) -> Vec<ClusterStateInfo> {
            self.fetch("/api/cluster/state").await
        }

        pub async fn sharding_all(&self) -> Vec<ShardingSnapshot> {
            self.fetch("/api/sharding").await
        }

        pub async fn persistence_all(&self) -> Vec<PersistenceSnapshot> {
            self.fetch("/api/persistence").await
        }

        pub async fn remote_all(&self) -> Vec<RemoteSnapshot> {
            self.fetch("/api/remote").await
        }

        pub async fn streams_all(&self) -> Vec<StreamsSnapshot> {
            self.fetch("/api/streams").await
        }

        pub async fn ddata_all(&self) -> Vec<DDataSnapshot> {
            self.fetch("/api/ddata").await
        }
    }

    /// Merge multiple [`OverviewSnapshot`]s into a cluster roll-up.
    pub fn merge_overviews(items: &[OverviewSnapshot]) -> OverviewSnapshot {
        let mut node = String::from("cluster");
        if let Some(first) = items.first() {
            node = format!("cluster[{}]", first.node);
        }
        OverviewSnapshot {
            node,
            generated_at: chrono::Utc::now().to_rfc3339(),
            actor_count: items.iter().map(|i| i.actor_count).sum(),
            dead_letter_count: items.iter().map(|i| i.dead_letter_count).sum(),
            cluster_member_count: items.iter().map(|i| i.cluster_member_count).max().unwrap_or(0),
            cluster_unreachable_count: items.iter().map(|i| i.cluster_unreachable_count).max().unwrap_or(0),
            remote_association_count: items.iter().map(|i| i.remote_association_count).sum(),
            running_graphs: items.iter().map(|i| i.running_graphs).sum(),
            persistence_event_count: items.iter().map(|i| i.persistence_event_count).sum(),
            ddata_key_count: items.iter().map(|i| i.ddata_key_count).max().unwrap_or(0),
        }
    }

    /// Merge per-node actor snapshots into a single flat + synthetic-roots
    /// snapshot where each root is `/node/<name>`.
    pub fn merge_actor_snapshots(per_node: &[(String, ActorSnapshot)]) -> ActorSnapshot {
        let mut flat: Vec<ActorStatus> = Vec::new();
        let mut roots = Vec::with_capacity(per_node.len());
        let mut total = 0u64;
        for (node, snap) in per_node {
            total += snap.total;
            flat.extend(snap.flat.iter().cloned());
            roots.push(atomr_telemetry::dto::ActorTreeNode {
                path: format!("/node/{node}"),
                name: node.clone(),
                actor_type: "Node".into(),
                mailbox_depth: 0,
                children: snap.roots.clone(),
            });
        }
        ActorSnapshot { total, roots, flat }
    }

    /// Build a cluster-wide [`ClusterStateInfo`] by union-ing members.
    pub fn merge_cluster_states(items: &[ClusterStateInfo]) -> ClusterStateInfo {
        let mut members_by_addr: std::collections::BTreeMap<String, ClusterMemberInfo> =
            std::collections::BTreeMap::new();
        let mut unreachable: std::collections::BTreeSet<String> = Default::default();
        for s in items {
            for m in &s.members {
                members_by_addr.insert(m.address.clone(), m.clone());
            }
            for u in &s.unreachable {
                unreachable.insert(u.clone());
            }
        }
        ClusterStateInfo {
            self_address: None,
            leader: items.iter().find_map(|s| s.leader.clone()),
            members: members_by_addr.into_values().collect(),
            unreachable: unreachable.into_iter().collect(),
            reachability_records: items.iter().flat_map(|s| s.reachability_records.clone()).collect(),
            gossip_version: items.iter().flat_map(|s| s.gossip_version.clone()).collect(),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use atomr_telemetry::dto::{ActorSnapshot, ClusterMemberInfo, OverviewSnapshot};

        fn ov(actors: u64, dl: u64) -> OverviewSnapshot {
            OverviewSnapshot {
                node: "n".into(),
                generated_at: "t".into(),
                actor_count: actors,
                dead_letter_count: dl,
                cluster_member_count: 1,
                cluster_unreachable_count: 0,
                remote_association_count: 0,
                running_graphs: 0,
                persistence_event_count: 0,
                ddata_key_count: 0,
            }
        }

        #[test]
        fn merge_overviews_sums_counters() {
            let merged = merge_overviews(&[ov(1, 2), ov(3, 4)]);
            assert_eq!(merged.actor_count, 4);
            assert_eq!(merged.dead_letter_count, 6);
        }

        #[test]
        fn merge_actor_snapshots_synthesises_node_roots() {
            let per_node = vec![
                ("a".to_string(), ActorSnapshot { total: 1, roots: vec![], flat: vec![] }),
                ("b".to_string(), ActorSnapshot { total: 2, roots: vec![], flat: vec![] }),
            ];
            let merged = merge_actor_snapshots(&per_node);
            assert_eq!(merged.total, 3);
            assert_eq!(merged.roots.len(), 2);
            assert!(merged.roots[0].path.starts_with("/node/"));
        }

        #[test]
        fn merge_cluster_states_unions_members() {
            let a = ClusterStateInfo {
                members: vec![ClusterMemberInfo {
                    address: "a".into(),
                    status: "Up".into(),
                    roles: vec![],
                    reachable: true,
                    up_number: 1,
                }],
                ..Default::default()
            };
            let b = ClusterStateInfo {
                members: vec![ClusterMemberInfo {
                    address: "b".into(),
                    status: "Up".into(),
                    roles: vec![],
                    reachable: true,
                    up_number: 2,
                }],
                ..Default::default()
            };
            let merged = merge_cluster_states(&[a, b]);
            assert_eq!(merged.members.len(), 2);
        }
    }
}

#[cfg(feature = "aggregator")]
pub use imp::*;

#[cfg(not(feature = "aggregator"))]
pub struct ClusterAggregator;
