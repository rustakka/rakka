//! MongoDB `SnapshotStore` implementation.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::{FindOptions, IndexOptions};
use mongodb::{Client, Collection, IndexModel};
use rustakka_persistence::{JournalError, SnapshotMetadata, SnapshotStore};

use crate::config::MongoConfig;
use crate::documents::SnapshotDoc;

pub struct MongoSnapshotStore {
    client: Client,
    cfg: MongoConfig,
}

impl MongoSnapshotStore {
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

    fn collection(&self) -> Collection<SnapshotDoc> {
        self.client
            .database(&self.cfg.database)
            .collection::<SnapshotDoc>(&self.cfg.snapshot_collection)
    }

    async fn ensure_indexes(&self) -> Result<(), JournalError> {
        let model = IndexModel::builder()
            .keys(doc! { "persistence_id": 1, "sequence_nr": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        self.collection().create_index(model).await.map_err(JournalError::backend)?;
        Ok(())
    }
}

#[async_trait]
impl SnapshotStore for MongoSnapshotStore {
    async fn save(&self, meta: SnapshotMetadata, payload: Vec<u8>) {
        let now = chrono::Utc::now().timestamp_millis();
        let doc = SnapshotDoc::from_meta(&meta, payload, now);
        let _ = self.collection().insert_one(doc).await;
    }

    async fn load(&self, persistence_id: &str) -> Option<(SnapshotMetadata, Vec<u8>)> {
        let opts = FindOptions::builder()
            .sort(doc! { "sequence_nr": -1 })
            .limit(1i64)
            .build();
        let mut cur = self
            .collection()
            .find(doc! { "persistence_id": persistence_id })
            .with_options(opts)
            .await
            .ok()?;
        let doc = cur.try_next().await.ok().flatten()?;
        Some(doc.into_parts())
    }

    async fn delete(&self, persistence_id: &str, to_sequence_nr: u64) {
        let _ = self
            .collection()
            .delete_many(doc! {
                "persistence_id": persistence_id,
                "sequence_nr": { "$lte": to_sequence_nr as i64 },
            })
            .await;
    }
}
