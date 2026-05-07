//! `atomr-pycluster-sharding` — placeholder sub-crate.
//!
//! The actual Python bindings for cluster sharding live in
//! `atomr-pycore` (`crates/py-bindings/pycore/src/ext_cluster_sharding.rs`).
//! This crate exists so the workspace mirrors the Rust crate layout
//! and a future split (separate wheels per subsystem) doesn't require
//! renaming the Python facade.
//!
//! The Python surface is exposed through `atomr._native.cluster_sharding`
//! and re-exported by `python/atomr/cluster_sharding.py`.

// Intentionally empty.
