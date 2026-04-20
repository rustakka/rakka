//! Connection configuration for the MongoDB provider.

use std::env;

#[derive(Debug, Clone)]
pub struct MongoConfig {
    pub url: String,
    pub database: String,
    pub journal_collection: String,
    pub snapshot_collection: String,
}

impl MongoConfig {
    pub fn new(url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            database: database.into(),
            journal_collection: "event_journal".into(),
            snapshot_collection: "snapshot_store".into(),
        }
    }

    pub fn with_collections(
        mut self,
        journal: impl Into<String>,
        snapshot: impl Into<String>,
    ) -> Self {
        self.journal_collection = journal.into();
        self.snapshot_collection = snapshot.into();
        self
    }

    /// Env lookup: `RUSTAKKA_PERSISTENCE_MONGO_URL`, `RUSTAKKA_IT_MONGO_URL`,
    /// `MONGODB_URL`, dev fallback `mongodb://127.0.0.1:27017`.
    pub fn from_env() -> Self {
        let url = env::var("RUSTAKKA_PERSISTENCE_MONGO_URL")
            .or_else(|_| env::var("RUSTAKKA_IT_MONGO_URL"))
            .or_else(|_| env::var("MONGODB_URL"))
            .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".to_string());
        let db = env::var("RUSTAKKA_PERSISTENCE_MONGO_DB")
            .unwrap_or_else(|_| "rustakka".into());
        Self::new(url, db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let cfg = MongoConfig::new("mongodb://x", "rustakka");
        assert_eq!(cfg.journal_collection, "event_journal");
        assert_eq!(cfg.snapshot_collection, "snapshot_store");
    }

    #[test]
    fn custom_collections() {
        let cfg = MongoConfig::new("mongodb://x", "db").with_collections("j", "s");
        assert_eq!(cfg.journal_collection, "j");
        assert_eq!(cfg.snapshot_collection, "s");
    }
}
