//! rustakka-distributed-data. akka.net: `src/contrib/cluster/Akka.DistributedData`.
//!
//! Provides core CRDTs (`GCounter`, `PNCounter`, `GSet`, `ORSet`, `LWWRegister`)
//! and a `Replicator` that stores them in-memory and merges on request.

mod counters;
mod register;
mod replicator;
mod sets;
mod traits;

pub use counters::{GCounter, PNCounter};
pub use register::LwwRegister;
pub use replicator::{ReadConsistency, Replicator, WriteConsistency};
pub use sets::{GSet, OrSet};
pub use traits::CrdtMerge;
