//! Connection configuration for the DynamoDB provider.

use std::env;

#[derive(Debug, Clone)]
pub struct DynamoConfig {
    pub table_name: String,
    /// Optional endpoint override for dynamodb-local.
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub auto_create_table: bool,
}

impl DynamoConfig {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self { table_name: table_name.into(), endpoint_url: None, region: None, auto_create_table: true }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_auto_create(mut self, create: bool) -> Self {
        self.auto_create_table = create;
        self
    }

    /// Env lookup: `RAKKA_PERSISTENCE_DYNAMO_TABLE`, endpoint override
    /// via `RAKKA_PERSISTENCE_DYNAMO_ENDPOINT` /
    /// `RAKKA_IT_DYNAMO_ENDPOINT` (dynamodb-local).
    pub fn from_env() -> Self {
        let table =
            env::var("RAKKA_PERSISTENCE_DYNAMO_TABLE").unwrap_or_else(|_| "rakka_persistence".to_string());
        let endpoint = env::var("RAKKA_PERSISTENCE_DYNAMO_ENDPOINT")
            .or_else(|_| env::var("RAKKA_IT_DYNAMO_ENDPOINT"))
            .ok();
        let region = env::var("AWS_REGION").ok();
        let mut cfg = Self::new(table);
        if let Some(e) = endpoint {
            cfg = cfg.with_endpoint(e);
        }
        if let Some(r) = region {
            cfg = cfg.with_region(r);
        }
        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_chain() {
        let cfg = DynamoConfig::new("t")
            .with_endpoint("http://localhost:8000")
            .with_region("us-east-1")
            .with_auto_create(false);
        assert_eq!(cfg.table_name, "t");
        assert_eq!(cfg.endpoint_url.as_deref(), Some("http://localhost:8000"));
        assert!(!cfg.auto_create_table);
    }
}
