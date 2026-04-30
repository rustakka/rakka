//! DynamoDB `Journal` implementation (single-table design).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use aws_sdk_dynamodb::Client;
use rakka_persistence::{Journal, JournalError, PersistentRepr};

use crate::config::DynamoConfig;
use crate::keys::{event_sk, parse_sequence, EVENT_PREFIX};
use crate::schema::ensure_table;

pub struct DynamoJournal {
    client: Client,
    cfg: DynamoConfig,
}

impl DynamoJournal {
    pub async fn connect(cfg: DynamoConfig) -> Result<Arc<Self>, JournalError> {
        let client = build_client(&cfg).await;
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

    fn to_av(&self, repr: &PersistentRepr) -> HashMap<String, AttributeValue> {
        let mut av = HashMap::new();
        av.insert("pid".into(), AttributeValue::S(repr.persistence_id.clone()));
        av.insert("sk".into(), AttributeValue::S(event_sk(repr.sequence_nr)));
        av.insert("seq".into(), AttributeValue::N(repr.sequence_nr.to_string()));
        av.insert("payload".into(), AttributeValue::B(Blob::new(repr.payload.clone())));
        av.insert("manifest".into(), AttributeValue::S(repr.manifest.clone()));
        av.insert("writer_uuid".into(), AttributeValue::S(repr.writer_uuid.clone()));
        av.insert("deleted".into(), AttributeValue::Bool(repr.deleted));
        if !repr.tags.is_empty() {
            av.insert(
                "tags".into(),
                AttributeValue::Ss(repr.tags.clone()),
            );
        }
        av
    }

    async fn current_max(&self, pid: &str) -> Result<u64, JournalError> {
        let out = self
            .client
            .query()
            .table_name(&self.cfg.table_name)
            .key_condition_expression("#p = :p AND begins_with(#s, :e)")
            .expression_attribute_names("#p", "pid")
            .expression_attribute_names("#s", "sk")
            .expression_attribute_values(":p", AttributeValue::S(pid.into()))
            .expression_attribute_values(":e", AttributeValue::S(EVENT_PREFIX.into()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .map_err(|e| JournalError::backend(format!("{e:?}")))?;
        let items = out.items();
        if items.is_empty() {
            return Ok(0);
        }
        let sk = items[0]
            .get("sk")
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        Ok(parse_sequence(&sk).unwrap_or(0))
    }
}

async fn build_client(cfg: &DynamoConfig) -> Client {
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
impl Journal for DynamoJournal {
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
                    return Err(JournalError::SequenceOutOfOrder {
                        expected,
                        got: msg.sequence_nr,
                    });
                }
                expected += 1;
                let item = self.to_av(&msg);
                self.client
                    .put_item()
                    .table_name(&self.cfg.table_name)
                    .set_item(Some(item))
                    .condition_expression("attribute_not_exists(sk)")
                    .send()
                    .await
                    .map_err(|e| JournalError::backend(format!("{e:?}")))?;
            }
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        for seq in 1..=to_sequence_nr {
            let mut key = HashMap::new();
            key.insert("pid".into(), AttributeValue::S(persistence_id.into()));
            key.insert("sk".into(), AttributeValue::S(event_sk(seq)));
            let _ = self
                .client
                .update_item()
                .table_name(&self.cfg.table_name)
                .set_key(Some(key))
                .update_expression("SET #d = :t")
                .expression_attribute_names("#d", "deleted")
                .expression_attribute_values(":t", AttributeValue::Bool(true))
                .return_values(ReturnValue::None)
                .send()
                .await;
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
        let limit = if max > i32::MAX as u64 { i32::MAX } else { max as i32 };
        let from_sk = event_sk(from);
        let to_sk = event_sk(to);
        let out = self
            .client
            .query()
            .table_name(&self.cfg.table_name)
            .key_condition_expression("#p = :p AND #s BETWEEN :from AND :to")
            .expression_attribute_names("#p", "pid")
            .expression_attribute_names("#s", "sk")
            .expression_attribute_values(":p", AttributeValue::S(persistence_id.into()))
            .expression_attribute_values(":from", AttributeValue::S(from_sk))
            .expression_attribute_values(":to", AttributeValue::S(to_sk))
            .limit(limit)
            .send()
            .await
            .map_err(|e| JournalError::backend(format!("{e:?}")))?;
        let mut results = Vec::new();
        for item in out.items() {
            let deleted = item.get("deleted").and_then(|v| v.as_bool().ok()).copied().unwrap_or(false);
            if deleted {
                continue;
            }
            let seq = item
                .get("seq")
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let payload = item
                .get("payload")
                .and_then(|v| v.as_b().ok())
                .map(|b| b.as_ref().to_vec())
                .unwrap_or_default();
            let manifest = item
                .get("manifest")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let writer_uuid = item
                .get("writer_uuid")
                .and_then(|v| v.as_s().ok())
                .cloned()
                .unwrap_or_default();
            let tags = item
                .get("tags")
                .and_then(|v| v.as_ss().ok())
                .map(|v| v.clone())
                .unwrap_or_default();
            results.push(PersistentRepr {
                persistence_id: persistence_id.to_string(),
                sequence_nr: seq,
                payload,
                manifest,
                writer_uuid,
                deleted,
                tags,
            });
        }
        Ok(results)
    }

    async fn highest_sequence_nr(
        &self,
        persistence_id: &str,
        _from: u64,
    ) -> Result<u64, JournalError> {
        self.current_max(persistence_id).await
    }
}
