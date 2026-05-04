//! MongoDB `Journal` implementation.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::{FindOptions, IndexOptions};
use mongodb::{Client, Collection, IndexModel};
use atomr_persistence::{Journal, JournalError, PersistentRepr};

use crate::config::MongoConfig;
use crate::documents::EventDoc;

pub struct MongoJournal {
    client: Client,
    cfg: MongoConfig,
}

impl MongoJournal {
    pub async fn connect(cfg: MongoConfig) -> Result<Arc<Self>, JournalError> {
        let client = Client::with_uri_str(&cfg.url).await.map_err(JournalError::backend)?;
        let me = Self { client, cfg };
        me.ensure_indexes().await?;
        Ok(Arc::new(me))
    }

    pub async fn from_client(client: Client, cfg: MongoConfig) -> Result<Arc<Self>, JournalError> {
        let me = Self { client, cfg };
        me.ensure_indexes().await?;
        Ok(Arc::new(me))
    }

    pub fn config(&self) -> &MongoConfig {
        &self.cfg
    }

    fn collection(&self) -> Collection<EventDoc> {
        self.client.database(&self.cfg.database).collection::<EventDoc>(&self.cfg.journal_collection)
    }

    async fn ensure_indexes(&self) -> Result<(), JournalError> {
        let model = IndexModel::builder()
            .keys(doc! { "persistence_id": 1, "sequence_nr": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        self.collection().create_index(model).await.map_err(JournalError::backend)?;
        let tag_model = IndexModel::builder().keys(doc! { "tags": 1, "sequence_nr": 1 }).build();
        self.collection().create_index(tag_model).await.map_err(JournalError::backend)?;
        Ok(())
    }

    async fn current_max(&self, pid: &str) -> Result<i64, JournalError> {
        let opts = FindOptions::builder().sort(doc! { "sequence_nr": -1 }).limit(1i64).build();
        let mut cur = self
            .collection()
            .find(doc! { "persistence_id": pid })
            .with_options(opts)
            .await
            .map_err(JournalError::backend)?;
        Ok(cur.try_next().await.map_err(JournalError::backend)?.map(|d| d.sequence_nr).unwrap_or(0))
    }
}

#[async_trait]
impl Journal for MongoJournal {
    async fn write_messages(&self, messages: Vec<PersistentRepr>) -> Result<(), JournalError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut by_pid: std::collections::BTreeMap<String, Vec<PersistentRepr>> =
            std::collections::BTreeMap::new();
        for m in messages {
            by_pid.entry(m.persistence_id.clone()).or_default().push(m);
        }
        let col = self.collection();
        let now = chrono::Utc::now().timestamp_millis();
        for (pid, batch) in by_pid {
            let mut expected = self.current_max(&pid).await? as u64 + 1;
            let mut docs = Vec::with_capacity(batch.len());
            for msg in batch {
                if msg.sequence_nr != expected {
                    return Err(JournalError::SequenceOutOfOrder { expected, got: msg.sequence_nr });
                }
                expected += 1;
                docs.push(EventDoc::from_repr(&msg, now));
            }
            col.insert_many(docs).await.map_err(JournalError::backend)?;
        }
        Ok(())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence_nr: u64,
    ) -> Result<(), JournalError> {
        self.collection()
            .update_many(
                doc! { "persistence_id": persistence_id, "sequence_nr": { "$lte": to_sequence_nr as i64 } },
                doc! { "$set": { "deleted": true } },
            )
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
        let limit = if max > i64::MAX as u64 { i64::MAX } else { max as i64 };
        let opts = FindOptions::builder().sort(doc! { "sequence_nr": 1 }).limit(limit).build();
        let mut cur = self
            .collection()
            .find(doc! {
                "persistence_id": persistence_id,
                "sequence_nr": { "$gte": from as i64, "$lte": clamp(to) },
                "deleted": false,
            })
            .with_options(opts)
            .await
            .map_err(JournalError::backend)?;
        let mut out = Vec::new();
        while let Some(doc) = cur.try_next().await.map_err(JournalError::backend)? {
            out.push(doc.into_repr());
        }
        Ok(out)
    }

    async fn highest_sequence_nr(&self, persistence_id: &str, _from: u64) -> Result<u64, JournalError> {
        Ok(self.current_max(persistence_id).await? as u64)
    }

    async fn events_by_tag(
        &self,
        tag: &str,
        from_offset: u64,
        max: u64,
    ) -> Result<Vec<PersistentRepr>, JournalError> {
        let limit = if max > i64::MAX as u64 { i64::MAX } else { max as i64 };
        let opts =
            FindOptions::builder().sort(doc! { "persistence_id": 1, "sequence_nr": 1 }).limit(limit).build();
        let mut cur = self
            .collection()
            .find(doc! {
                "tags": tag,
                "sequence_nr": { "$gte": from_offset as i64 },
                "deleted": false,
            })
            .with_options(opts)
            .await
            .map_err(JournalError::backend)?;
        let mut out = Vec::new();
        while let Some(doc) = cur.try_next().await.map_err(JournalError::backend)? {
            out.push(doc.into_repr());
        }
        Ok(out)
    }
}

fn clamp(v: u64) -> i64 {
    if v > i64::MAX as u64 {
        i64::MAX
    } else {
        v as i64
    }
}
