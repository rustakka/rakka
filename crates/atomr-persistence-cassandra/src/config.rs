//! Connection configuration for the Cassandra provider.

use std::env;

pub const DEFAULT_PARTITION_SIZE: u64 = 1_000_000;

#[derive(Debug, Clone)]
pub struct CassandraConfig {
    pub nodes: Vec<String>,
    pub keyspace: String,
    pub replication: String,
    pub partition_size: u64,
    pub journal_table: String,
    pub snapshot_table: String,
}

impl CassandraConfig {
    pub fn new(nodes: Vec<String>, keyspace: impl Into<String>) -> Self {
        Self {
            nodes,
            keyspace: keyspace.into(),
            replication: "{'class': 'SimpleStrategy', 'replication_factor': 1}".into(),
            partition_size: DEFAULT_PARTITION_SIZE,
            journal_table: "event_journal".into(),
            snapshot_table: "snapshot_store".into(),
        }
    }

    pub fn with_replication(mut self, r: impl Into<String>) -> Self {
        self.replication = r.into();
        self
    }

    pub fn with_partition_size(mut self, size: u64) -> Self {
        self.partition_size = size.max(1);
        self
    }

    pub fn partition_for(&self, seq: u64) -> i64 {
        ((seq.saturating_sub(1)) / self.partition_size) as i64
    }

    /// Env lookup: `ATOMR_PERSISTENCE_CASSANDRA_NODES` (comma separated),
    /// `ATOMR_IT_CASSANDRA_NODES`, `CASSANDRA_NODES`, dev fallback
    /// `127.0.0.1:9042`.
    pub fn from_env() -> Self {
        let nodes = env::var("ATOMR_PERSISTENCE_CASSANDRA_NODES")
            .or_else(|_| env::var("ATOMR_IT_CASSANDRA_NODES"))
            .or_else(|_| env::var("CASSANDRA_NODES"))
            .unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let keyspace = env::var("ATOMR_PERSISTENCE_CASSANDRA_KEYSPACE").unwrap_or_else(|_| "atomr".into());
        let nodes = nodes.split(',').map(|s| s.trim().to_string()).collect();
        Self::new(nodes, keyspace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_buckets() {
        let cfg = CassandraConfig::new(vec!["127.0.0.1".into()], "k").with_partition_size(10);
        assert_eq!(cfg.partition_for(1), 0);
        assert_eq!(cfg.partition_for(10), 0);
        assert_eq!(cfg.partition_for(11), 1);
        assert_eq!(cfg.partition_for(20), 1);
        assert_eq!(cfg.partition_for(21), 2);
    }

    #[test]
    fn defaults() {
        let cfg = CassandraConfig::new(vec!["h".into()], "k");
        assert_eq!(cfg.partition_size, DEFAULT_PARTITION_SIZE);
    }
}
