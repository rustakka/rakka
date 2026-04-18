//! Owns the shard→region allocation table. akka.net: `PersistentShardCoordinator`.

use std::collections::HashMap;

use parking_lot::RwLock;

#[derive(Default)]
pub struct ShardCoordinator {
    allocation: RwLock<HashMap<String, String>>,
}

impl ShardCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the region hosting `shard_id`, allocating it to `default_region`
    /// on first mention. Mirrors the "least-shards" allocation of akka.net
    /// where the caller supplies candidate regions.
    pub fn allocate(&self, shard_id: &str, default_region: &str) -> String {
        let mut map = self.allocation.write();
        map.entry(shard_id.to_string()).or_insert_with(|| default_region.to_string()).clone()
    }

    pub fn region_for(&self, shard_id: &str) -> Option<String> {
        self.allocation.read().get(shard_id).cloned()
    }

    pub fn rebalance(&self, shard_id: &str, to_region: impl Into<String>) {
        self.allocation.write().insert(shard_id.to_string(), to_region.into());
    }

    pub fn shard_count(&self) -> usize {
        self.allocation.read().len()
    }
}
