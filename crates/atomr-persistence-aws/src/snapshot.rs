//! DynamoDB `SnapshotStore` implementation (single-table design).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use atomr_persistence::{JournalError, SnapshotMetadata, SnapshotStore};

use crate::config::DynamoConfig;
use crate::keys::{parse_sequence, snapshot_sk, SNAPSHOT_PREFIX};
use crate::schema::ensure_table;

pub struct DynamoSnapshotStore {
    client: Client,
    cfg: DynamoConfig,
}

impl DynamoSnapshotStore {
    pub async fn connect(cfg: DynamoConfig) -> Result<Arc<Self>, JournalError> {
        let client = super_build_client(&cfg).await;
        ensure_table(&client, &cfg).await?;
        Ok(Arc::new(Self { client, cfg }))
    }

    pub async fn from_client(client: Client, cfg: DynamoConfig) -> Result<Arc<Self>, JournalError> {
        ensure_table(&client, &cfg).await?;
        Ok(Arc::new(Self { client, cfg }))
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn config(&self) -> &DynamoConfig {
        &self.cfg
    }
}

async fn super_build_client(cfg: &DynamoConfig) -> Client {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = &cfg.region {
        loader = loader.region(aws_config::Region::new(region.clone()));
    }
    let sdk_cfg = loader.load().await;
    let mut builder = aws_sdk_dynamodb::config::Builder::from(&sdk_cfg);
    if let Some(endpoint) = &cfg.endpoint_url {
        builder = builder.endpoint_url(endpoint);
    }
    Client::from_conf(builder.build())
}

#[async_trait]
impl SnapshotStore for DynamoSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let mut item = HashMap::new();
        item.insert("pid".into(), AttributeValue::S(meta.persistence_id.clone()));
        item.insert("sk".into(), AttributeValue::S(snapshot_sk(meta.sequence_nr)));
        item.insert("seq".into(), AttributeValue::N(meta.sequence_nr.to_string()));
        item.insert("payload".into(), AttributeValue::B(Blob::new(payload)));
        item.insert("timestamp".into(), AttributeValue::N(meta.timestamp.to_string()));
        let _ = self.client.put_item().table_name(&self.cfg.table_name).set_item(Some(item)).send().await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let out = self
            .client
            .query()
            .table_name(&self.cfg.table_name)
            .key_condition_expression("#p = :p AND begins_with(#s, :prefix)")
            .expression_attribute_names("#p", "pid")
            .expression_attribute_names("#s", "sk")
            .expression_attribute_values(":p", AttributeValue::S(persistence_id.into()))
            .expression_attribute_values(":prefix", AttributeValue::S(SNAPSHOT_PREFIX.into()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .ok()?;
        let item = out.items().first()?;
        let sk = item.get("sk")?.as_s().ok()?.clone();
        let seq = parse_sequence(&sk)?;
        let payload = item.get("payload")?.as_b().ok()?.as_ref().to_vec();
        let timestamp = item
            .get("timestamp")
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Some((
            SnapshotMetadata { persistence_id: persistence_id.to_string(), sequence_nr: seq, timestamp },
            payload,
        ))
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        for seq in 1..=to_sequence_nr {
            let mut key = HashMap::new();
            key.insert("pid".into(), AttributeValue::S(persistence_id.into()));
            key.insert("sk".into(), AttributeValue::S(snapshot_sk(seq)));
            let _ =
                self.client.delete_item().table_name(&self.cfg.table_name).set_key(Some(key)).send().await;
        }
    }
}
