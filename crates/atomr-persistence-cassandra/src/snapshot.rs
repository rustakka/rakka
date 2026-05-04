//! Cassandra `SnapshotStore` implementation.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{JournalError, SnapshotMetadata, SnapshotStore};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use crate::config::CassandraConfig;
use crate::schema::ensure_schema;

pub struct CassandraSnapshotStore {
    session: Arc<Session>,
    cfg: CassandraConfig,
}

impl CassandraSnapshotStore {
    pub async fn connect(cfg: CassandraConfig) -> Result<Arc<Self>, JournalError> {
        let mut builder = SessionBuilder::new();
        for node in &cfg.nodes {
            builder = builder.known_node(node);
        }
        let session = builder.build().await.map_err(JournalError::backend)?;
        ensure_schema(&session, &cfg).await?;
        Ok(Arc::new(Self { session: Arc::new(session), cfg }))
    }

    pub async fn from_session(
        session: Arc<Session>,
        cfg: CassandraConfig,
    ) -> Result<Arc<Self>, JournalError> {
        ensure_schema(&session, &cfg).await?;
        Ok(Arc::new(Self { session, cfg }))
    }

    pub fn config(&self) -> &CassandraConfig {
        &self.cfg
    }
}

#[async_trait]
impl SnapshotStore for CassandraSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let cql = format!(
            "INSERT INTO {ks}.{table} (persistence_id, sequence_nr, payload, timestamp) VALUES (?, ?, ?, ?)",
            ks = self.cfg.keyspace,
            table = self.cfg.snapshot_table,
        );
        let _ = self
            .session
            .query_unpaged(
                cql,
                (&meta.persistence_id, meta.sequence_nr as i64, payload, meta.timestamp as i64),
            )
            .await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let cql = format!(
            "SELECT sequence_nr, payload, timestamp FROM {ks}.{table} \
             WHERE persistence_id = ? LIMIT 1",
            ks = self.cfg.keyspace,
            table = self.cfg.snapshot_table,
        );
        let rows = self.session.query_unpaged(cql, (persistence_id,)).await.ok()?;
        let rows = rows.into_rows_result().ok()?;
        let mut iter = rows.rows::<(i64, Vec<u8>, i64)>().ok()?;
        let (seq, payload, ts) = iter.next()?.ok()?;
        Some((
            SnapshotMetadata {
                persistence_id: persistence_id.to_string(),
                sequence_nr: seq as u64,
                timestamp: ts as u64,
            },
            payload,
        ))
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        let cql = format!(
            "DELETE FROM {ks}.{table} WHERE persistence_id = ? AND sequence_nr <= ?",
            ks = self.cfg.keyspace,
            table = self.cfg.snapshot_table,
        );
        // Cassandra doesn't support range deletes without a clustering range
        // prefix in the same way RDBMS do, so we enumerate sequence numbers
        // first.
        let list_cql = format!(
            "SELECT sequence_nr FROM {ks}.{table} \
             WHERE persistence_id = ? AND sequence_nr <= ?",
            ks = self.cfg.keyspace,
            table = self.cfg.snapshot_table,
        );
        let rows = match self
            .session
            .query_unpaged(list_cql, (persistence_id, to_sequence_nr as i64))
            .await
            .ok()
            .and_then(|r| r.into_rows_result().ok())
        {
            Some(r) => r,
            None => return,
        };
        let iter = match rows.rows::<(i64,)>() {
            Ok(i) => i,
            Err(_) => return,
        };
        for (seq,) in iter.flatten() {
            let _ = self.session.query_unpaged(cql.as_str(), (persistence_id, seq)).await;
        }
    }
}
