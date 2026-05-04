//! Azure Table Storage connection configuration.

use std::env;

use atomr_persistence::JournalError;

/// Azurite developer account / key defaults.
pub const AZURITE_ACCOUNT: &str = "devstoreaccount1";
pub const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
pub const AZURITE_ENDPOINT: &str = "http://127.0.0.1:10002/devstoreaccount1";

#[derive(Debug, Clone)]
pub struct AzureConfig {
    pub account: String,
    pub key: String,
    pub endpoint: String,
    pub journal_table: String,
    pub snapshot_table: String,
    pub auto_create_tables: bool,
}

impl AzureConfig {
    pub fn new(account: impl Into<String>, key: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            account: account.into(),
            key: key.into(),
            endpoint: endpoint.into(),
            journal_table: "EventJournal".into(),
            snapshot_table: "SnapshotStore".into(),
            auto_create_tables: true,
        }
    }

    /// Point the config at a local Azurite instance.
    pub fn azurite() -> Self {
        Self::new(AZURITE_ACCOUNT, AZURITE_KEY, AZURITE_ENDPOINT)
    }

    /// Connection-string style input, e.g.
    /// `AccountName=...;AccountKey=...;TableEndpoint=...`.
    pub fn from_connection_string(cs: &str) -> Result<Self, JournalError> {
        let mut account = None;
        let mut key = None;
        let mut endpoint = None;
        for part in cs.split(';').filter(|p| !p.is_empty()) {
            let (k, v) =
                part.split_once('=').ok_or_else(|| JournalError::backend("malformed connection string"))?;
            match k.trim() {
                "AccountName" => account = Some(v.to_string()),
                "AccountKey" => key = Some(v.to_string()),
                "TableEndpoint" => endpoint = Some(v.to_string()),
                _ => {}
            }
        }
        let account = account.ok_or_else(|| JournalError::backend("missing AccountName"))?;
        let key = key.ok_or_else(|| JournalError::backend("missing AccountKey"))?;
        let endpoint = endpoint.unwrap_or_else(|| format!("https://{account}.table.core.windows.net"));
        Ok(Self::new(account, key, endpoint))
    }

    pub fn from_env() -> Self {
        if let Ok(cs) = env::var("ATOMR_PERSISTENCE_AZURE_CONNECTION_STRING") {
            if let Ok(cfg) = Self::from_connection_string(&cs) {
                return cfg;
            }
        }
        if let Ok(cs) = env::var("ATOMR_IT_AZURE_CONNECTION_STRING") {
            if let Ok(cfg) = Self::from_connection_string(&cs) {
                return cfg;
            }
        }
        Self::azurite()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connection_string() {
        let cs = "AccountName=acc;AccountKey=key;TableEndpoint=https://acc.table.core.windows.net";
        let cfg = AzureConfig::from_connection_string(cs).unwrap();
        assert_eq!(cfg.account, "acc");
        assert_eq!(cfg.key, "key");
        assert_eq!(cfg.endpoint, "https://acc.table.core.windows.net");
    }

    #[test]
    fn rejects_bad_connection_string() {
        assert!(AzureConfig::from_connection_string("no-equals-here").is_err());
    }

    #[test]
    fn azurite_defaults() {
        let cfg = AzureConfig::azurite();
        assert_eq!(cfg.account, AZURITE_ACCOUNT);
        assert!(cfg.endpoint.starts_with("http://"));
    }
}
