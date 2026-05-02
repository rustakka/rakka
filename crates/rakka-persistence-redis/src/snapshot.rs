//! Snapshot store backed by a Redis sorted set keyed by sequence number.

use std::sync::Arc;

use async_trait::async_trait;
use fred::prelude::*;
use rakka_persistence::{JournalError, SnapshotMetadata, SnapshotStore};

use crate::codec::StoredSnapshot;
use crate::config::RedisConfig;

pub struct RedisSnapshotStore {
    client: Pool,
    cfg: RedisConfig,
}

impl RedisSnapshotStore {
    pub async fn connect(cfg: RedisConfig) -> Result<Arc<Self>, JournalError> {
        let builder = Builder::from_config(Config::from_url(&cfg.url).map_err(JournalError::backend)?);
        let pool = builder.build_pool(cfg.pool_size).map_err(JournalError::backend)?;
        pool.init().await.map_err(JournalError::backend)?;
        Ok(Arc::new(Self { client: pool, cfg }))
    }

    pub fn from_pool(pool: Pool, cfg: RedisConfig) -> Arc<Self> {
        Arc::new(Self { client: pool, cfg })
    }

    pub fn config(&self) -> &RedisConfig {
        &self.cfg
    }

    pub fn client(&self) -> &Pool {
        &self.client
    }
}

#[async_trait]
impl SnapshotStore for RedisSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let key = self.cfg.snapshot_key(&meta.persistence_id);
        let stored = StoredSnapshot::new(&meta, &payload);
        let raw = match serde_json::to_string(&stored) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot encode failed");
                return;
            }
        };
        let _: Result<(), _> =
            self.client.zadd(&key, None, None, false, false, (meta.sequence_nr as f64, raw)).await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let key = self.cfg.snapshot_key(persistence_id);
        let res: Result<Vec<(String, f64)>, _> =
            self.client.zrange(&key, -1, -1, None, false, None, true).await;
        let members = res.ok()?;
        let (raw, _) = members.into_iter().next()?;
        let stored: StoredSnapshot = serde_json::from_str(&raw).ok()?;
        Some(stored.into_parts())
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        let key = self.cfg.snapshot_key(persistence_id);
        let _: Result<i64, _> = self.client.zremrangebyscore(&key, 0.0, to_sequence_nr as f64).await;
    }
}
