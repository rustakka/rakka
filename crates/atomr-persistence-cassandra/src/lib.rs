//! atomr-persistence-cassandra.
//!
//! Events are stored in a wide-row table partitioned by
//! `(persistence_id, partition_nr)` so large persistence ids stay within
//! Cassandra partition size limits. Snapshots live in a sibling table
//! keyed by `persistence_id + sequence_nr`.

mod config;
mod journal;
mod schema;
mod snapshot;

pub use config::{CassandraConfig, DEFAULT_PARTITION_SIZE};
pub use journal::CassandraJournal;
pub use schema::ensure_schema;
pub use snapshot::CassandraSnapshotStore;
