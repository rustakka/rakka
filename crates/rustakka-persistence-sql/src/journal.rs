//! `Journal` implementation backed by sqlx.
//!
//! Uses the `sqlx::Any` pool so the same code targets every supported
//! dialect. Tag writes go to a companion `event_tags` table that powers
//! `events_by_tag`.

use std::sync::Arc;

use async_trait::async_trait;
use rustakka_persistence::{Journal, JournalError, PersistentRepr};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;

use crate::config::SqlConfig;
use crate::schema::{ensure_schema, init_drivers};

/// Saturating cast from `u64` to `i64` so `u64::MAX` sentinels turn into
/// `i64::MAX` instead of wrapping negative.
fn clamp_i64(v: u64) -> i64 {
    if v > i64::MAX as u64 {
        i64::MAX
    } else {
        v as i64
    }
}

pub struct SqlJournal {
    pool: AnyPool,
    cfg: SqlConfig,
}

impl SqlJournal {
    /// Connect, install drivers, and optionally run migrations.
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

    /// Reuse an existing pool (for tests or app-wide sharing).
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

    async fn current_highest(&self, pid: &str) -> Result<u64, JournalError> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT MAX(sequence_nr) FROM event_journal WHERE persistence_id = ?")
                .bind(pid)
                .fetch_optional(&self.pool)
                .await
                .map_err(JournalError::backend)?;
        Ok(row.and_then(|(v,)| v).map(|v| v as u64).unwrap_or(0))
    }
}

#[async_trait]
impl Journal for SqlJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(JournalError::backend)?;
        let mut by_pid: std::collections::BTreeMap<String, Vec<PersistentRepr>> =
            std::collections::BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }
        for (pid, batch) in by_pid {
            let row: Option<(Option<i64>,)> = sqlx::query_as(
                "SELECT MAX(sequence_nr) FROM event_journal WHERE persistence_id = ?",
            )
            .bind(&pid)
            .fetch_optional(&mut *tx)
            .await
            .map_err(JournalError::backend)?;
            let mut expected = row
                .and_then(|(v,)| v)
                .map(|v| v as u64 + 1)
                .unwrap_or(1);
            for msg in batch {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder {
                        expected,
                        got: msg.sequence_nr,
                    });
                }
                let created_at = chrono::Utc::now().timestamp_millis();
                sqlx::query(
                    "INSERT INTO event_journal (persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&msg.persistence_id)
                .bind(msg.sequence_nr as i64)
                .bind(msg.payload.clone())
                .bind(&msg.manifest)
                .bind(&msg.writer_uuid)
                .bind(0i32)
                .bind(created_at)
                .execute(&mut *tx)
                .await
                .map_err(JournalError::backend)?;
                for tag in &msg.tags {
                    sqlx::query(
                        "INSERT INTO event_tags (persistence_id, sequence_nr, tag) VALUES (?, ?, ?)",
                    )
                    .bind(&msg.persistence_id)
                    .bind(msg.sequence_nr as i64)
                    .bind(tag)
                    .execute(&mut *tx)
                    .await
                    .map_err(JournalError::backend)?;
                }
                expected += 1;
            }
        }
        tx.commit().await.map_err(JournalError::backend)?;
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        sqlx::query(
            "UPDATE event_journal SET deleted = 1 WHERE persistence_id = ? AND sequence_nr <= ?",
        )
        .bind(persistence_id)
        .bind(to_sequence_nr as i64)
        .execute(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let limit = clamp_i64(max);
        let to_bound = clamp_i64(to);
        let from_bound = clamp_i64(from);
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT persistence_id, sequence_nr, payload, manifest, writer_uuid, deleted FROM event_journal \
             WHERE persistence_id = ? AND sequence_nr >= ? AND sequence_nr <= ? AND deleted = 0 \
             ORDER BY sequence_nr ASC LIMIT ?",
        )
        .bind(persistence_id)
        .bind(from_bound)
        .bind(to_bound)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for (pid, seq, payload, manifest, writer, deleted) in rows {
            let tags: Vec<(String,)> = sqlx::query_as(
                "SELECT tag FROM event_tags WHERE persistence_id = ? AND sequence_nr = ?",
            )
            .bind(&pid)
            .bind(seq)
            .fetch_all(&self.pool)
            .await
            .map_err(JournalError::backend)?;
            out.push(PersistentRepr {
                persistence_id: pid,
                sequence_nr: seq as u64,
                payload,
                manifest,
                writer_uuid: writer,
                deleted: deleted != 0,
                tags: tags.into_iter().map(|(t,)| t).collect(),
            });
        }
        Ok(out)
    }

    async fn highest_sequence_nr(
        &self,
        persistence_id: &str,
        _from_sequence_nr: u64,
    ) -> Result<u64, JournalError> {
        self.current_highest(persistence_id).await
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let limit = clamp_i64(max);
        let rows: Vec<(String, i64, Vec<u8>, String, String, i32)> = sqlx::query_as(
            "SELECT j.persistence_id, j.sequence_nr, j.payload, j.manifest, j.writer_uuid, j.deleted \
             FROM event_journal j INNER JOIN event_tags t \
             ON j.persistence_id = t.persistence_id AND j.sequence_nr = t.sequence_nr \
             WHERE t.tag = ? AND j.sequence_nr >= ? AND j.deleted = 0 \
             ORDER BY j.persistence_id, j.sequence_nr ASC LIMIT ?",
        )
        .bind(tag)
        .bind(clamp_i64(from_offset))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(JournalError::backend)?;
        Ok(rows
            .into_iter()
            .map(|(pid, seq, payload, manifest, writer, deleted)| PersistentRepr {
                persistence_id: pid,
                sequence_nr: seq as u64,
                payload,
                manifest,
                writer_uuid: writer,
                deleted: deleted != 0,
                tags: vec![tag.to_string()],
            })
            .collect())
    }
}
