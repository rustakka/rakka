//! rakka-persistence-mongodb. akka.net: `Akka.Persistence.MongoDB`.
//!
//! Stores events in a MongoDB collection keyed by `(persistence_id,
//! sequence_nr)` with a unique compound index. Snapshots live in a
//! sibling collection with the same compound key.

mod config;
mod documents;
mod journal;
mod snapshot;

pub use config::MongoConfig;
pub use documents::{EventDoc, SnapshotDoc};
pub use journal::MongoJournal;
pub use snapshot::MongoSnapshotStore;
