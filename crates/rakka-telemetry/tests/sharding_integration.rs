//! Integration test that snapshots live `ShardRegion` / `ShardCoordinator`
//! instances through the sharding probe.

#![cfg(feature = "sharding")]

use std::sync::Arc;

use rakka_cluster_sharding::{MessageExtractor, ShardCoordinator, ShardRegion};
use rakka_telemetry::bus::TelemetryBus;
use rakka_telemetry::sharding::ShardingProbe;

struct Ex;
impl MessageExtractor for Ex {
    type Message = (String, u32);
    fn entity_id(&self, m: &Self::Message) -> String {
        m.0.clone()
    }
    fn shard_id(&self, m: &Self::Message) -> String {
        format!("shard-{}", (m.0.len() % 4))
    }
}

#[test]
fn snapshots_live_region_and_coordinator() {
    let coordinator = Arc::new(ShardCoordinator::new());
    let region = ShardRegion::new(
        "r1",
        Arc::new(Ex),
        coordinator.clone(),
        Arc::new(|| Box::new(|_id: &str, _msg: (String, u32)| {})),
    );

    region.deliver(("alice".into(), 1));
    region.deliver(("bob".into(), 2));

    let probe = ShardingProbe::new(TelemetryBus::new(8));
    probe.refresh_from(&[region.as_ref()], coordinator.as_ref());

    let snap = probe.snapshot();
    assert_eq!(snap.regions.len(), 1);
    assert_eq!(snap.regions[0].region_id, "r1");
    assert!(snap.regions[0].shard_count >= 1);
    assert!(!snap.allocations.is_empty());
}
