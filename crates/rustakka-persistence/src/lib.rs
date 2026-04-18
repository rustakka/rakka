//! rustakka-persistence. akka.net: `src/core/Akka.Persistence`.
//!
//! Event-sourced persistent actor model with pluggable journal and snapshot stores.

mod alod;
mod journal;
mod persistent_actor;
mod recovery;
mod snapshot;

pub use alod::{AtLeastOnceDelivery, UnconfirmedDelivery};
pub use journal::{InMemoryJournal, Journal, JournalError, PersistentRepr};
pub use persistent_actor::PersistentActor;
pub use recovery::{Recovery, RecoveryState};
pub use snapshot::{InMemorySnapshotStore, SnapshotMetadata, SnapshotStore};
