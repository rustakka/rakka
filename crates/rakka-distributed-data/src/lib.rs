//! rakka-distributed-data. akka.net: `src/contrib/cluster/Akka.DistributedData`.
//!
//! Provides CRDTs (`GCounter`, `PNCounter`, `GSet`, `ORSet`, `LWWRegister`,
//! `Flag`, `ORMap`, `LWWMap`, `PNCounterMap`) and a `Replicator` that
//! stores them in-memory and merges on request.

mod counters;
mod flag;
mod maps;
mod register;
mod replicator;
mod sets;
mod traits;

pub use counters::{GCounter, PNCounter};
pub use flag::Flag;
pub use maps::{LWWMap, ORMap, ORMultiMap, PNCounterMap};
pub use register::LwwRegister;
pub use replicator::{ReadConsistency, Replicator, SubscriptionToken, WriteConsistency};
pub use sets::{GSet, OrSet};
pub use traits::{CrdtMerge, DeltaCrdt};
