//! Remember-entities — persist active entity ids so they restart on
//! shard re-allocation.
//!
//! Phase 9.G of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Cluster.Sharding.RememberEntities`.
//!
//! [`RememberedEntities`] is the in-memory book-keeping layer
//! (per-shard entity-id sets). [`RememberEntitiesStore`] is the
//! pluggable trait the shard region calls to persist / load the
//! set across restarts. The default [`InMemoryRememberStore`] is
//! suitable for tests; production shard regions wire a journal- or
//! ddata-backed implementation.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use parking_lot::RwLock;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RememberError {
    #[error("backend error: {0}")]
    Backend(String),
}

/// Pluggable persistence store for remembered entities.
#[async_trait]
pub trait RememberEntitiesStore: Send + Sync + 'static {
    /// Load the full entity-id set for `shard_id`.
    async fn load(&self, shard_id: &str) -> Result<HashSet<String>, RememberError>;

    /// Persist that `entity_id` is now active in `shard_id`.
    async fn add(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError>;

    /// Persist that `entity_id` is no longer active in `shard_id`
    /// (typically after passivation).
    async fn remove(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError>;
}

/// In-process registry of remembered entity ids. Wraps a
/// [`RememberEntitiesStore`] and serves quick lookups from a local
/// snapshot.
pub struct RememberedEntities {
    store: std::sync::Arc<dyn RememberEntitiesStore>,
    cache: RwLock<HashMap<String, HashSet<String>>>, // shard_id -> ids
}

impl RememberedEntities {
    pub fn new(store: std::sync::Arc<dyn RememberEntitiesStore>) -> Self {
        Self { store, cache: RwLock::new(HashMap::new()) }
    }

    /// Refresh cache from the backing store. Idempotent.
    pub async fn warm(&self, shard_id: &str) -> Result<(), RememberError> {
        let ids = self.store.load(shard_id).await?;
        self.cache.write().insert(shard_id.into(), ids);
        Ok(())
    }

    /// Mark `entity_id` active. Updates the cache and the store.
    pub async fn record_active(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError> {
        self.store.add(shard_id, entity_id).await?;
        self.cache.write().entry(shard_id.into()).or_default().insert(entity_id.into());
        Ok(())
    }

    /// Mark `entity_id` inactive (passivated/stopped).
    pub async fn record_inactive(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError> {
        self.store.remove(shard_id, entity_id).await?;
        if let Some(set) = self.cache.write().get_mut(shard_id) {
            set.remove(entity_id);
        }
        Ok(())
    }

    /// Snapshot of currently-known entity ids for `shard_id`.
    pub fn entities(&self, shard_id: &str) -> HashSet<String> {
        self.cache.read().get(shard_id).cloned().unwrap_or_default()
    }

    pub fn shard_count(&self) -> usize {
        self.cache.read().len()
    }
}

/// In-memory store — for tests and as a reference implementation.
#[derive(Default)]
pub struct InMemoryRememberStore {
    inner: RwLock<HashMap<String, HashSet<String>>>,
}

impl InMemoryRememberStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RememberEntitiesStore for InMemoryRememberStore {
    async fn load(&self, shard_id: &str) -> Result<HashSet<String>, RememberError> {
        Ok(self.inner.read().get(shard_id).cloned().unwrap_or_default())
    }

    async fn add(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError> {
        self.inner.write().entry(shard_id.into()).or_default().insert(entity_id.into());
        Ok(())
    }

    async fn remove(&self, shard_id: &str, entity_id: &str) -> Result<(), RememberError> {
        if let Some(set) = self.inner.write().get_mut(shard_id) {
            set.remove(entity_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn record_and_warm_round_trip() {
        let store: Arc<dyn RememberEntitiesStore> = Arc::new(InMemoryRememberStore::new());
        let r = RememberedEntities::new(store.clone());

        r.record_active("s1", "e1").await.unwrap();
        r.record_active("s1", "e2").await.unwrap();
        r.record_active("s2", "e3").await.unwrap();

        // Fresh registry recovers from the store.
        let r2 = RememberedEntities::new(store);
        r2.warm("s1").await.unwrap();
        let ids = r2.entities("s1");
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("e1") && ids.contains("e2"));
    }

    #[tokio::test]
    async fn record_inactive_drops_from_set() {
        let store: Arc<dyn RememberEntitiesStore> = Arc::new(InMemoryRememberStore::new());
        let r = RememberedEntities::new(store);
        r.record_active("s1", "e1").await.unwrap();
        r.record_active("s1", "e2").await.unwrap();
        r.record_inactive("s1", "e1").await.unwrap();
        let ids = r.entities("s1");
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("e2"));
    }

    #[tokio::test]
    async fn shard_count_tracks_distinct_shards() {
        let store: Arc<dyn RememberEntitiesStore> = Arc::new(InMemoryRememberStore::new());
        let r = RememberedEntities::new(store);
        r.record_active("s1", "e1").await.unwrap();
        r.record_active("s2", "e2").await.unwrap();
        r.record_active("s3", "e3").await.unwrap();
        assert_eq!(r.shard_count(), 3);
    }
}
