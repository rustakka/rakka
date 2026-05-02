//! Azure Table Storage `Journal`.

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{Journal, JournalError, PersistentRepr};

use crate::config::AzureConfig;
use crate::entities::EventEntity;
use crate::rest::TableClient;

pub struct AzureJournal {
    client: TableClient,
    cfg: AzureConfig,
}

impl AzureJournal {
    pub async fn connect(cfg: AzureConfig) -> Result<Arc<Self>, JournalError> {
        let client = TableClient::new(&cfg.endpoint, &cfg.account, &cfg.key)?;
        if cfg.auto_create_tables {
            client.create_table_if_absent(&cfg.journal_table).await?;
        }
        Ok(Arc::new(Self { client, cfg }))
    }

    pub fn config(&self) -> &AzureConfig {
        &self.cfg
    }

    async fn current_max(&self, pid: &str) -> Result<u64, JournalError> {
        let filter = format!("PartitionKey eq '{pid}'");
        let entities: Vec<EventEntity> =
            self.client.query_entities(&self.cfg.journal_table, &filter, None).await?;
        Ok(entities.into_iter().map(|e| e.sequence_nr as u64).max().unwrap_or(0))
    }
}

fn escape_pk(pid: &str) -> String {
    pid.replace('\'', "''")
}

#[async_trait]
impl Journal for AzureJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut by_pid: std::collections::BTreeMap<String, Vec<PersistentRepr>> =
            std::collections::BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }
        for (pid, batch) in by_pid {
            let mut expected = self.current_max(&pid).await? + 1;
            for msg in batch {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder { expected, got: msg.sequence_nr });
                }
                expected += 1;
                let entity = EventEntity::from_repr(&msg);
                self.client.insert_entity(&self.cfg.journal_table, &entity).await?;
            }
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        let pk = escape_pk(persistence_id);
        let filter = format!("PartitionKey eq '{pk}' and SequenceNr le {to}", to = to_sequence_nr as i64);
        let entities: Vec<EventEntity> =
            self.client.query_entities(&self.cfg.journal_table, &filter, None).await?;
        for mut entity in entities {
            entity.deleted = true;
            self.client
                .upsert_entity(
                    &self.cfg.journal_table,
                    &entity.partition_key.clone(),
                    &entity.row_key.clone(),
                    &entity,
                )
                .await?;
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
        let pk = escape_pk(persistence_id);
        let to_bound = to.min(i64::MAX as u64) as i64;
        let filter = format!(
            "PartitionKey eq '{pk}' and SequenceNr ge {from} and SequenceNr le {to_bound} and Deleted eq false",
            from = from as i64,
        );
        let top = if max > u32::MAX as u64 { None } else { Some(max as u32) };
        let mut entities: Vec<EventEntity> =
            self.client.query_entities(&self.cfg.journal_table, &filter, top).await?;
        entities.sort_by_key(|e| e.sequence_nr);
        let limit = if max > usize::MAX as u64 { usize::MAX } else { max as usize };
        Ok(entities.into_iter().take(limit).map(EventEntity::into_repr).collect())
    }

    async fn highest_sequence_nr(&self, persistence_id: &str, _from: u64) -> Result<u64, JournalError> {
        self.current_max(persistence_id).await
    }
}
