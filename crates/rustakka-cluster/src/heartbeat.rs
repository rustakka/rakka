//! Cluster heartbeat state. akka.net: `Cluster/ClusterHeartbeat.cs`.

use std::collections::HashMap;

use rustakka_core::actor::Address;

use rustakka_remote::PhiAccrualFailureDetector;

pub struct HeartbeatState {
    pub detectors: HashMap<Address, PhiAccrualFailureDetector>,
}

impl Default for HeartbeatState {
    fn default() -> Self {
        Self { detectors: HashMap::new() }
    }
}

impl HeartbeatState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn heartbeat(&mut self, from: Address) {
        self.detectors
            .entry(from)
            .or_insert_with(|| {
                PhiAccrualFailureDetector::new(
                    8.0,
                    1000,
                    std::time::Duration::from_millis(100),
                    std::time::Duration::from_secs(3),
                    std::time::Duration::from_secs(1),
                )
            })
            .heartbeat_on_proxy();
    }
}

// Helper — PhiAccrualFailureDetector has `heartbeat` via trait; use it.
// We need to call FailureDetector::heartbeat — provide a tiny helper.
trait _HeartbeatOnProxy {
    fn heartbeat_on_proxy(&self);
}

impl _HeartbeatOnProxy for PhiAccrualFailureDetector {
    fn heartbeat_on_proxy(&self) {
        use rustakka_remote::FailureDetector;
        FailureDetector::heartbeat(self);
    }
}
