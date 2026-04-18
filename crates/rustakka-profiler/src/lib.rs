//! Actor performance profiler for the rustakka Rust runtime.
//!
//! Exposes a handful of scenarios (`tell`, `ask`, `fanout`, `cpu`) that
//! exercise the mailbox, ask pattern, actor creation, and a CPU-bound
//! handler respectively. Each scenario emits a [`Measurement`] with the
//! same schema the Python profiler produces so the two runtimes can be
//! compared directly.
//!
//! See the `rustakka-profiler` binary for the CLI wrapper and
//! `scripts/profile.py` for the cross-runtime orchestrator.

pub mod metrics;
pub mod report;
pub mod scenarios;

pub use report::{Measurement, ProfilerReport, Scenario};
