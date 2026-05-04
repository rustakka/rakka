//! # atomr
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
//! atomr = { version = "0.1", features = ["cluster", "persistence", "streams"] }
//! ```
//!
//! ```ignore
//! use atomr::prelude::*;
//! use atomr::cluster;          // re-export of `atomr-cluster`
//! use atomr::persistence;      // re-export of `atomr-persistence`
//! use atomr::streams;          // re-export of `atomr-streams`
//! ```
//!
//! # Quick start
//!
//! ```ignore
//! use atomr::prelude::*;
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
//! | `macros` (default) | `atomr::macros` | `#[derive(Actor)]`, `props!`, `#[derive(Receive)]` |
//! | `testkit` | `atomr::testkit` | Probes, virtual time, multi-node spec |
//! | `remote` | `atomr::remote` | Cross-process / cross-host messaging |
//! | `cluster` | `atomr::cluster` | Membership, gossip, reachability, SBR |
//! | `cluster-tools` | `atomr::cluster_tools` | Singleton, distributed pub/sub, cluster client |
//! | `cluster-sharding` | `atomr::cluster_sharding` | Sharded entities with rebalance |
//! | `cluster-metrics` | `atomr::cluster_metrics` | Adaptive load balancing |
//! | `distributed-data` | `atomr::distributed_data` | CRDT replicator |
//! | `persistence` | `atomr::persistence` | Event sourcing — journals + snapshots |
//! | `persistence-query` | `atomr::persistence_query` | Tagged event streams |
//! | `streams` | `atomr::streams` | Typed reactive streams DSL |
//! | `coordination` | `atomr::coordination` | Lease primitives |
//! | `discovery` | `atomr::discovery` | Service discovery |
//! | `di` | `atomr::di` | DI container |
//! | `hosting` | `atomr::hosting` | Builder API |
//! | `telemetry` | `atomr::telemetry` | Tracing, metrics, exporters |
//! | `full` | (everything above) | Every subsystem + macros + testkit |
//! | `cluster-app` | (cluster-grade subset) | Cluster-grade application bundle |
//!
//! Each subsystem also publishes as its own crate (`atomr-cluster`,
//! `atomr-persistence`, …). If you only need one, depending on it
//! directly avoids the umbrella's resolver indirection.
//!
//! See the [repository README] for architecture and the
//! [actors-and-agentic-computing] doc for the unified-compute thesis.
//!
//! [repository README]: https://github.com/rustakka/atomr
//! [actors-and-agentic-computing]: https://github.com/rustakka/atomr/blob/main/docs/actors-and-agentic-computing.md

#![cfg_attr(docsrs, feature(doc_cfg))]

pub use atomr_config as config;
pub use atomr_core as core;

#[cfg(feature = "macros")]
#[cfg_attr(docsrs, doc(cfg(feature = "macros")))]
pub use atomr_macros as macros;

#[cfg(feature = "testkit")]
#[cfg_attr(docsrs, doc(cfg(feature = "testkit")))]
pub use atomr_testkit as testkit;

#[cfg(feature = "remote")]
#[cfg_attr(docsrs, doc(cfg(feature = "remote")))]
pub use atomr_remote as remote;

#[cfg(feature = "cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
pub use atomr_cluster as cluster;

#[cfg(feature = "cluster-tools")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-tools")))]
pub use atomr_cluster_tools as cluster_tools;

#[cfg(feature = "cluster-sharding")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-sharding")))]
pub use atomr_cluster_sharding as cluster_sharding;

#[cfg(feature = "cluster-metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-metrics")))]
pub use atomr_cluster_metrics as cluster_metrics;

#[cfg(feature = "distributed-data")]
#[cfg_attr(docsrs, doc(cfg(feature = "distributed-data")))]
pub use atomr_distributed_data as distributed_data;

#[cfg(feature = "persistence")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence")))]
pub use atomr_persistence as persistence;

#[cfg(feature = "persistence-query")]
#[cfg_attr(docsrs, doc(cfg(feature = "persistence-query")))]
pub use atomr_persistence_query as persistence_query;

#[cfg(feature = "streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
pub use atomr_streams as streams;

#[cfg(feature = "coordination")]
#[cfg_attr(docsrs, doc(cfg(feature = "coordination")))]
pub use atomr_coordination as coordination;

#[cfg(feature = "discovery")]
#[cfg_attr(docsrs, doc(cfg(feature = "discovery")))]
pub use atomr_discovery as discovery;

#[cfg(feature = "di")]
#[cfg_attr(docsrs, doc(cfg(feature = "di")))]
pub use atomr_di as di;

#[cfg(feature = "hosting")]
#[cfg_attr(docsrs, doc(cfg(feature = "hosting")))]
pub use atomr_hosting as hosting;

#[cfg(feature = "telemetry")]
#[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
pub use atomr_telemetry as telemetry;

/// Re-exports of the most commonly used types.
///
/// ```ignore
/// use atomr::prelude::*;
/// ```
pub mod prelude {
    pub use atomr_config::Config;
    pub use atomr_core::actor::{Actor, ActorRef, ActorSystem, Context, Props};
    pub use atomr_core::pattern::{ask, pipe_to};
    pub use atomr_core::supervision::{Directive, OneForOneStrategy, SupervisorStrategy};

    #[cfg(feature = "macros")]
    pub use atomr_macros::actor_msg;
}
