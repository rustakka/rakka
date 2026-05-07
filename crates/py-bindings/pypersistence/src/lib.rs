//! Placeholder — Python bindings for the persistence subsystem live in
//! `atomr-pycore::ext_persistence`.
//!
//! Phase 4 of the Python-bindings expansion adds the
//! `EventSourcedActor` Python base class plus `Effect` /
//! `InMemorySnapshotStore` / `RecoveryPermitter` shims. The orchestration
//! lives in `python/atomr/persistence.py`; the Rust pyclasses are
//! shipped from `atomr-pycore` so the assembled `_native` extension is a
//! single shared object. This crate exists to mirror the Rust workspace
//! structure so individual wheels can be carved out later without
//! renaming the Python facade.
