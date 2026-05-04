//! atomr-persistence-redis. akka.net: `Akka.Persistence.Redis`.
//!
//! Stores each persistence-id's journal as a sorted set keyed by sequence
//! number and each snapshot stream as a secondary sorted set keyed the same
//! way. Writes use MULTI/EXEC so a batch lands atomically.

mod codec;
mod config;
mod journal;
mod snapshot;

pub use codec::{StoredRepr, StoredSnapshot};
pub use config::RedisConfig;
pub use journal::RedisJournal;
pub use snapshot::RedisSnapshotStore;
