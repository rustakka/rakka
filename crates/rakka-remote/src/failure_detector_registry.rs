//! Per-`Address` failure detector registry.
//! akka.net: `Remote/FailureDetectorRegistry.cs`,
//! `Remote/DefaultFailureDetectorRegistry.cs`.

use std::sync::Arc;

use dashmap::DashMap;

use rakka_core::actor::Address;

use crate::failure_detector::FailureDetector;

/// Factory closure that produces a fresh detector for a newly-tracked
/// address. Default implementations build a `PhiAccrualFailureDetector`
/// with conservative thresholds.
type DetectorFactory = Arc<dyn Fn() -> Arc<dyn FailureDetector> + Send + Sync>;

#[derive(Clone)]
pub struct FailureDetectorRegistry {
    factory: DetectorFactory,
    detectors: Arc<DashMap<String, Arc<dyn FailureDetector>>>,
}

impl FailureDetectorRegistry {
    pub fn new(factory: DetectorFactory) -> Self {
        Self { factory, detectors: Arc::new(DashMap::new()) }
    }

    /// Default detector: `PhiAccrualFailureDetector` with phi threshold 8,
    /// 1000-sample window, ~100ms heartbeat, 3s acceptable pause, 1s warm-up.
    pub fn default_phi() -> Self {
        Self::new(Arc::new(|| {
            Arc::new(crate::phi_accrual::PhiAccrualFailureDetector::new(
                8.0,
                1000,
                std::time::Duration::from_millis(100),
                std::time::Duration::from_secs(3),
                std::time::Duration::from_secs(1),
            ))
        }))
    }

    pub fn heartbeat(&self, from: &Address) {
        let key = from.to_string();
        let entry = self
            .detectors
            .entry(key)
            .or_insert_with(|| (self.factory)());
        entry.heartbeat();
    }

    pub fn is_available(&self, address: &Address) -> bool {
        self.detectors
            .get(&address.to_string())
            .map(|d| d.is_available())
            .unwrap_or(true)
    }

    pub fn remove(&self, address: &Address) {
        self.detectors.remove(&address.to_string());
    }

    pub fn addresses(&self) -> Vec<String> {
        self.detectors.iter().map(|e| e.key().clone()).collect()
    }
}
