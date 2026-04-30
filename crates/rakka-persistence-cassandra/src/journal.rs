//! Cassandra `Journal` implementation. Partitions are keyed by
//! `(persistence_id, partition_nr)` so a single row-set stays below
//! Cassandra's recommended partition size.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{Journal, JournalError, PersistentRepr};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use crate::config::CassandraConfig;
use crate::schema::ensure_schema;

pub struct CassandraJournal {
    session: Arc<Session>,
    cfg: CassandraConfig,
}

impl CassandraJournal {
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

    async fn current_max(&self, pid: &str) -> Result<u64, JournalError> {
        // Walk partitions upward from 0 because the CQL primary key is
        // `(persistence_id, partition_nr)` and secondary indexes would be
        // prohibitively expensive. Sequences are dense within a persistence
        // id so the first partition with no rows terminates the scan.
        let cql = format!(
            "SELECT sequence_nr FROM {ks}.{table} \
             WHERE persistence_id = ? AND partition_nr = ? \
             ORDER BY sequence_nr DESC LIMIT 1",
            ks = self.cfg.keyspace,
            table = self.cfg.journal_table,
        );
        let prepared = self.session.prepare(cql).await.map_err(JournalError::backend)?;
        let mut partition: i64 = 0;
        let mut latest: u64 = 0;
        loop {
            let rows = self
                .session
                .execute_unpaged(&prepared, (pid, partition))
                .await
                .map_err(JournalError::backend)?
                .into_rows_result()
                .map_err(JournalError::backend)?;
            let mut iter = rows.rows::<(i64,)>().map_err(JournalError::backend)?;
            match iter.next() {
                Some(row) => {
                    let (seq,) = row.map_err(JournalError::backend)?;
                    latest = seq as u64;
                    partition += 1;
                }
                None => break,
            }
        }
        Ok(latest)
    }
}

#[async_trait]
impl Journal for CassandraJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut by_pid: BTreeMap<String, Vec<PersistentRepr>> = BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }
        let insert_cql = format!(
            "INSERT INTO {ks}.{table} \
             (persistence_id, partition_nr, sequence_nr, payload, manifest, writer_uuid, deleted, tags, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ks = self.cfg.keyspace,
            table = self.cfg.journal_table,
        );
        let prepared =
            self.session.prepare(insert_cql).await.map_err(JournalError::backend)?;
        let now = chrono::Utc::now().timestamp_millis();
        for (pid, batch) in by_pid {
            let mut expected = self.current_max(&pid).await? + 1;
            for msg in batch {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder {
                        expected,
                        got: msg.sequence_nr,
                    });
                }
                let partition = self.cfg.partition_for(msg.sequence_nr);
                let tag_set: HashSet<String> = msg.tags.iter().cloned().collect();
                self.session
                    .execute_unpaged(
                        &prepared,
                        (
                            &msg.persistence_id,
                            partition,
                            msg.sequence_nr as i64,
                            msg.payload.clone(),
                            &msg.manifest,
                            &msg.writer_uuid,
                            false,
                            &tag_set,
                            now,
                        ),
                    )
                    .await
                    .map_err(JournalError::backend)?;
                expected += 1;
            }
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        let cql = format!(
            "UPDATE {ks}.{table} SET deleted = true \
             WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr = ?",
            ks = self.cfg.keyspace,
            table = self.cfg.journal_table,
        );
        let prepared = self.session.prepare(cql).await.map_err(JournalError::backend)?;
        for seq in 1..=to_sequence_nr {
            let partition = self.cfg.partition_for(seq);
            self.session
                .execute_unpaged(&prepared, (persistence_id, partition, seq as i64))
                .await
                .map_err(JournalError::backend)?;
        }
        Ok(())
    }

    async fn replay_messages(
        &self,
        persistence_id: &str,
        from: u64,
        to: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let mut out = Vec::new();
        if from > to {
            return Ok(out);
        }
        let start_partition = self.cfg.partition_for(from.max(1));
        let end_partition = self.cfg.partition_for(to.min(u64::MAX - 1));
        let cql = format!(
            "SELECT sequence_nr, payload, manifest, writer_uuid, deleted, tags \
             FROM {ks}.{table} \
             WHERE persistence_id = ? AND partition_nr = ? \
             AND sequence_nr >= ? AND sequence_nr <= ?",
            ks = self.cfg.keyspace,
            table = self.cfg.journal_table,
        );
        let prepared = self.session.prepare(cql).await.map_err(JournalError::backend)?;
        for partition in start_partition..=end_partition {
            if out.len() as u64 >= max {
                break;
            }
            let remaining = max - out.len() as u64;
            let to_bound = (to as i64).min(i64::MAX);
            let rows = self
                .session
                .execute_unpaged(
                    &prepared,
                    (persistence_id, partition, from as i64, to_bound),
                )
                .await
                .map_err(JournalError::backend)?
                .into_rows_result()
                .map_err(JournalError::backend)?;
            let iter = rows
                .rows::<(i64, Vec<u8>, String, String, bool, Option<HashSet<String>>)>()
                .map_err(JournalError::backend)?;
            for row in iter.take(remaining as usize) {
                let (seq, payload, manifest, writer_uuid, deleted, tags) =
                    row.map_err(JournalError::backend)?;
                if deleted {
                    continue;
                }
                out.push(PersistentRepr {
                    persistence_id: persistence_id.to_string(),
                    sequence_nr: seq as u64,
                    payload,
                    manifest,
                    writer_uuid,
                    deleted,
                    tags: tags.map(|t| t.into_iter().collect()).unwrap_or_default(),
                });
                if out.len() as u64 >= max {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn highest_sequence_nr(
        &self,
        persistence_id: &str,
        _from: u64,
    ) -> Result<u64, JournalError> {
        self.current_max(persistence_id).await
    }
}
