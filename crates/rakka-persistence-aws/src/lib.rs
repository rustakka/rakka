//! rakka-persistence-aws. akka.net: `Akka.Persistence.DynamoDB`.
//!
//! Single-table design:
//! - partition key `pid` (S) = persistence id
//! - sort key `sk` (S) = prefixed sequence number, zero-padded so
//!   lexicographic ordering matches numeric ordering.
//!
//! Events use the `E#` prefix, snapshots use `S#`.

mod config;
mod journal;
mod keys;
mod schema;
mod snapshot;

pub use config::DynamoConfig;
pub use journal::DynamoJournal;
pub use keys::{event_sk, snapshot_sk, EVENT_PREFIX, SNAPSHOT_PREFIX};
pub use schema::ensure_table;
pub use snapshot::DynamoSnapshotStore;
