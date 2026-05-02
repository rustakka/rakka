//! Azure Table Storage `SnapshotStore`.

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{JournalError, SnapshotMetadata, SnapshotStore};

use crate::config::AzureConfig;
use crate::entities::SnapshotEntity;
use crate::rest::TableClient;

pub struct AzureSnapshotStore {
    client: TableClient,
    cfg: AzureConfig,
}

impl AzureSnapshotStore {
    pub async fn connect(cfg: AzureConfig) -> Result<Arc<Self>, JournalError> {
        let client = TableClient::new(&cfg.endpoint, &cfg.account, &cfg.key)?;
        if cfg.auto_create_tables {
            client.create_table_if_absent(&cfg.snapshot_table).await?;
        }
        Ok(Arc::new(Self { client, cfg }))
    }

    pub fn config(&self) -> &AzureConfig {
        &self.cfg
    }
}

fn escape_pk(pid: &str) -> String {
    pid.replace('\'', "''")
}

#[async_trait]
impl SnapshotStore for AzureSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let entity = SnapshotEntity::from_meta(&meta, &payload);
        let _ = self
            .client
            .upsert_entity(
                &self.cfg.snapshot_table,
                &entity.partition_key.clone(),
                &entity.row_key.clone(),
                &entity,
            )
            .await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let pk = escape_pk(persistence_id);
        let filter = format!("PartitionKey eq '{pk}'");
        let mut entities: Vec<SnapshotEntity> =
            self.client.query_entities(&self.cfg.snapshot_table, &filter, None).await.ok()?;
        entities.sort_by_key(|e| std::cmp::Reverse(e.sequence_nr));
        let entity = entities.into_iter().next()?;
        Some(entity.into_parts())
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        let pk = escape_pk(persistence_id);
        let filter = format!("PartitionKey eq '{pk}' and SequenceNr le {to}", to = to_sequence_nr as i64,);
        let entities: Vec<SnapshotEntity> =
            match self.client.query_entities(&self.cfg.snapshot_table, &filter, None).await {
                Ok(e) => e,
                Err(_) => return,
            };
        for entity in entities {
            let _ = self
                .client
                .delete_entity(&self.cfg.snapshot_table, &entity.partition_key, &entity.row_key)
                .await;
        }
    }
}
