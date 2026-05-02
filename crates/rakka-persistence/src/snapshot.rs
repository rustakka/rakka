//! Snapshot store plugin. akka.net: `SnapshotStore`, `MemorySnapshotStore`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub timestamp: u64,
}

#[async_trait]
pub trait SnapshotStore: Send + Sync + 'static {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>);
    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)>;
    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64);
}

type SnapshotEntries = HashMap<String, Vec<(SnapshotMetadata, Vec<u8>)>>;

#[derive(Default)]
pub struct InMemorySnapshotStore {
    snapshots: RwLock<SnapshotEntries>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl SnapshotStore for InMemorySnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        self.snapshots.write().entry(meta.persistence_id.clone()).or_default().push((meta, payload));
    }

    async fn load(&self, pid: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        self.snapshots.read().get(pid).and_then(|v| v.last()).cloned()
    }

    async fn delete(&self, pid: &str, to_sequence_nr: u64) {
        if let Some(v) = self.snapshots.write().get_mut(pid) {
            v.retain(|(m, _)| m.sequence_nr > to_sequence_nr);
        }
    }
}
