//! Sharding probe — snapshots [`ShardRegion`] / [`ShardCoordinator`] state
//! when the `sharding` feature is enabled.

use parking_lot::RwLock;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{ShardingEvent, ShardingSnapshot};
#[cfg(any(feature = "sharding", test))]
use crate::dto::ShardRegionInfo;

pub struct ShardingProbe {
    bus: TelemetryBus,
    snapshot: RwLock<ShardingSnapshot>,
}

impl ShardingProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self { bus, snapshot: RwLock::new(ShardingSnapshot::default()) }
    }

    pub fn set_snapshot(&self, snap: ShardingSnapshot) {
        *self.snapshot.write() = snap;
    }

    pub fn snapshot(&self) -> ShardingSnapshot {
        self.snapshot.read().clone()
    }

    pub fn record_shard_event(&self, region_id: &str, shard_id: &str, event: &str) {
        self.bus.publish(TelemetryEvent::ShardingChanged(ShardingEvent {
            region_id: region_id.to_string(),
            shard_id: shard_id.to_string(),
            event: event.to_string(),
        }));
    }
}

/// Build a [`ShardRegionInfo`] from a live `rakka-cluster-sharding`
/// region. Feature-gated.
#[cfg(feature = "sharding")]
pub fn region_info<E: rakka_cluster_sharding::MessageExtractor>(
    region: &rakka_cluster_sharding::ShardRegion<E>,
) -> ShardRegionInfo {
    ShardRegionInfo {
        region_id: region.region_id().to_string(),
        shard_count: region.shard_count(),
        shards: region.shard_ids(),
    }
}

/// Snapshot of the coordinator's shard → region allocation table.
#[cfg(feature = "sharding")]
pub fn coordinator_allocations(
    coord: &rakka_cluster_sharding::ShardCoordinator,
) -> Vec<(String, String)> {
    coord.allocations()
}

impl ShardingProbe {
    /// Convenience: refresh the probe snapshot from a list of live
    /// regions and a coordinator.
    #[cfg(feature = "sharding")]
    pub fn refresh_from<E: rakka_cluster_sharding::MessageExtractor>(
        &self,
        regions: &[&rakka_cluster_sharding::ShardRegion<E>],
        coordinator: &rakka_cluster_sharding::ShardCoordinator,
    ) {
        let regions = regions.iter().map(|r| region_info(*r)).collect();
        let allocations = coordinator_allocations(coordinator);
        self.set_snapshot(ShardingSnapshot { regions, allocations });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_snapshot_and_event() {
        let bus = TelemetryBus::new(8);
        let mut rx = bus.subscribe();
        let probe = ShardingProbe::new(bus);
        probe.set_snapshot(ShardingSnapshot {
            regions: vec![ShardRegionInfo {
                region_id: "r1".into(),
                shard_count: 3,
                shards: vec!["s1".into()],
            }],
            allocations: vec![],
        });
        assert_eq!(probe.snapshot().regions[0].shard_count, 3);
        probe.record_shard_event("r1", "s1", "started");
        let e = rx.recv().await.unwrap();
        assert_eq!(e.topic(), "sharding");
    }
}
