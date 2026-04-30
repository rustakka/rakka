//! rakka-persistence-azure. akka.net: `Akka.Persistence.Azure`.
//!
//! Default backend is Azure Table Storage (feature `tables`), implemented
//! with a thin REST client so the crate stays compatible with both real
//! Azure and the Azurite emulator. A `cosmos` feature placeholder is
//! reserved for the Cosmos SQL API implementation.

mod auth;
mod config;
mod entities;
mod journal;
mod rest;
mod snapshot;

pub use auth::SharedKeySigner;
pub use config::AzureConfig;
pub use entities::{EventEntity, SnapshotEntity};
pub use journal::AzureJournal;
pub use snapshot::AzureSnapshotStore;

#[cfg(feature = "cosmos")]
pub mod cosmos {
    //! Placeholder for the Cosmos SQL API provider. Uses the same
    //! `Journal` + `SnapshotStore` shape as the Tables impl but with
    //! Cosmos-specific partitioning semantics.
    pub struct CosmosJournal;
    pub struct CosmosSnapshotStore;
}
