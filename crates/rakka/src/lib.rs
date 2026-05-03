//! # rakka
//!
//! A native Rust runtime for **actor-based concurrent and distributed
//! systems**. One programming model — addressable units of state plus
//! behavior, communicating by asynchronous messages — that scales from
//! a single core to a cluster, and increasingly from CPU to GPU.
//!
//! This is the **umbrella crate**. Pull in additional subsystems via
//! Cargo feature flags; each feature also re-exports the underlying
//! crate under a stable namespace.
//!
//! ```toml
//! [dependencies]
//! # Cargo's `package =` alias keeps the import name `rakka`
//! # while the published crate is `rakka-rs` (the short name on
//! # crates.io is owned by an unrelated dormant crate).
//! rakka = { package = "rakka-rs", version = "0.2", features = ["cluster", "persistence", "streams"] }
//! ```
//!
//! ```ignore
//! use rakka::prelude::*;
//! use rakka::cluster;          // re-export of `rakka-cluster`
//! use rakka::persistence;      // re-export of `rakka-persistence`
//! use rakka::streams;          // re-export of `rakka-streams`
//! ```
//!
//! # Quick start
//!
//! ```ignore
//! use rakka::prelude::*;
//!
//! #[derive(Default)]
//! struct Greeter;
//!
//! #[async_trait::async_trait]
//! impl Actor for Greeter {
//!     type Msg = String;
//!     async fn handle(&mut self, _ctx: &mut Context<Self>, msg: String) {
//!         println!("hi {msg}");
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let system = ActorSystem::create("S", Config::empty()).await?;
//! let greeter = system.actor_of(Props::create(Greeter::default), "greeter")?;
//! greeter.tell("world".to_string());
//! system.terminate().await;
//! # Ok(()) }
//! ```
//!
//! # Feature flags
//!
//! | Feature | Re-exports as | What it adds |
//! |---|---|---|
//! | `macros` (default) | `rakka::macros` | `#[derive(Actor)]`, `props!`, `#[derive(Receive)]` |
//! | `testkit` | `rakka::testkit` | Probes, virtual time, multi-node spec |
//! | `remote` | `rakka::remote` | Cross-process / cross-host messaging |
//! | `cluster` | `rakka::cluster` | Membership, gossip, reachability, SBR |
//! | `cluster-tools` | `rakka::cluster_tools` | Singleton, distributed pub/sub, cluster client |
//! | `cluster-sharding` | `rakka::cluster_sharding` | Sharded entities with rebalance |
//! | `cluster-metrics` | `rakka::cluster_metrics` | Adaptive load balancing |
//! | `distributed-data` | `rakka::distributed_data` | CRDT replicator |
//! | `persistence` | `rakka::persistence` | Event sourcing — journals + snapshots |
//! | `persistence-query` | `rakka::persistence_query` | Tagged event streams |
//! | `streams` | `rakka::streams` | Typed reactive streams DSL |
//! | `coordination` | `rakka::coordination` | Lease primitives |
//! | `discovery` | `rakka::discovery` | Service discovery |
//! | `di` | `rakka::di` | DI container |
//! | `hosting` | `rakka::hosting` | Builder API |
//! | `telemetry` | `rakka::telemetry` | Tracing, metrics, exporters |
//! | `full` | (everything above) | Every subsystem + macros + testkit |
//! | `cluster-app` | (cluster-grade subset) | Cluster-grade application bundle |
//!
//! Each subsystem also publishes as its own crate (`rakka-cluster`,
//! `rakka-persistence`, …). If you only need one, depending on it
//! directly avoids the umbrella's resolver indirection.
//!
//! See the [repository README] for architecture and the
//! [actors-and-agentic-computing] doc for the unified-compute thesis.
//!
//! [repository README]: https://github.com/rustakka/rakka
//! [actors-and-agentic-computing]: https://github.com/rustakka/rakka/blob/main/docs/actors-and-agentic-computing.md

#![cfg_attr(docsrs, feature(doc_cfg))]

pub use rakka_config as config;
pub use rakka_core as core;

#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
pub use rakka_macros as macros;

#[cfg(feature = "testkit")]
#[cfg_attr(docsrs, doc(cfg(feature = "testkit")))]
pub use rakka_testkit as testkit;

#[cfg(feature = "remote")]
#[cfg_attr(docsrs, doc(cfg(feature = "remote")))]
pub use rakka_remote as remote;

#[cfg(feature = "cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
pub use rakka_cluster as cluster;

#[cfg(feature = "cluster-tools")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-tools")))]
pub use rakka_cluster_tools as cluster_tools;

#[cfg(feature = "cluster-sharding")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-sharding")))]
pub use rakka_cluster_sharding as cluster_sharding;

#[cfg(feature = "cluster-metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-metrics")))]
pub use rakka_cluster_metrics as cluster_metrics;

#[cfg(feature = "distributed-data")]
#[cfg_attr(docsrs, doc(cfg(feature = "distributed-data")))]
pub use rakka_distributed_data as distributed_data;

#[cfg(feature = "persistence")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
pub use rakka_persistence as persistence;

#[cfg(feature = "persistence-query")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence-query")))]
pub use rakka_persistence_query as persistence_query;

#[cfg(feature = "streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
pub use rakka_streams as streams;

#[cfg(feature = "coordination")]
#[cfg_attr(docsrs, doc(cfg(feature = "coordination")))]
pub use rakka_coordination as coordination;

#[cfg(feature = "discovery")]
#[cfg_attr(docsrs, doc(cfg(feature = "discovery")))]
pub use rakka_discovery as discovery;

#[cfg(feature = "di")]
#[cfg_attr(docsrs, doc(cfg(feature = "di")))]
pub use rakka_di as di;

#[cfg(feature = "hosting")]
#[cfg_attr(docsrs, doc(cfg(feature = "hosting")))]
pub use rakka_hosting as hosting;

#[cfg(feature = "telemetry")]
#[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
pub use rakka_telemetry as telemetry;

/// Re-exports of the most commonly used types.
///
/// ```ignore
/// use rakka::prelude::*;
/// ```
pub mod prelude {
    pub use rakka_config::Config;
    pub use rakka_core::actor::{Actor, ActorRef, ActorSystem, Context, Props};
    pub use rakka_core::pattern::{ask, pipe_to};
    pub use rakka_core::supervision::{Directive, OneForOneStrategy, SupervisorStrategy};

    #[cfg(feature = "macros")]
    pub use rakka_macros::actor_msg;
}
