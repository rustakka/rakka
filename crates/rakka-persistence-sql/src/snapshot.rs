//! `SnapshotStore` implementation backed by sqlx.

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{JournalError, SnapshotMetadata, SnapshotStore};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

use crate::config::SqlConfig;
use crate::schema::{ensure_schema, init_drivers};

pub struct SqlSnapshotStore {
    pool: AnyPool,
    cfg: SqlConfig,
}

impl SqlSnapshotStore {
    pub async fn connect(cfg: SqlConfig) -> Result<Arc<Self>, JournalError> {
        init_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(cfg.max_connections)
            .connect(&cfg.url)
            .await
            .map_err(JournalError::backend)?;
        ensure_schema(&pool, &cfg).await?;
        Ok(Arc::new(Self { pool, cfg }))
    }

    pub async fn from_pool(pool: AnyPool, cfg: SqlConfig) -> Result<Arc<Self>, JournalError> {
        ensure_schema(&pool, &cfg).await?;
        Ok(Arc::new(Self { pool, cfg }))
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    pub fn config(&self) -> &SqlConfig {
        &self.cfg
    }
}

#[async_trait]
impl SnapshotStore for SqlSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let created_at = chrono::Utc::now().timestamp_millis();
        let _ = sqlx::query(
            "INSERT INTO snapshot_store (persistence_id, sequence_nr, payload, timestamp, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&meta.persistence_id)
        .bind(meta.sequence_nr as i64)
        .bind(payload)
        .bind(meta.timestamp as i64)
        .bind(created_at)
        .execute(&self.pool)
        .await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let row: Option<(String, i64, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT persistence_id, sequence_nr, payload, timestamp FROM snapshot_store \
             WHERE persistence_id = ? ORDER BY sequence_nr DESC LIMIT 1",
        )
        .bind(persistence_id)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        row.map(|(pid, seq, payload, ts)| {
            (
                SnapshotMetadata {
                    persistence_id: pid,
                    sequence_nr: seq as u64,
                    timestamp: ts as u64,
                },
                payload,
            )
        })
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        let _ = sqlx::query(
            "DELETE FROM snapshot_store WHERE persistence_id = ? AND sequence_nr <= ?",
        )
        .bind(persistence_id)
        .bind(to_sequence_nr as i64)
        .execute(&self.pool)
        .await;
    }
}
