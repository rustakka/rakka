//! atomr-pyddata — placeholder marker crate. The actual PyO3 wrappers
//! for [`atomr_distributed_data`] live in `atomr-pycore` (under
//! `ext_ddata.rs`); this crate re-exports a small set of helper aliases
//! so that downstream code referencing the per-subsystem crate name has
//! a stable Rust target until we carve out a separate cdylib wheel.
//!
//! The Python facade lives at `python/atomr/ddata.py`; the symbols
//! enumerated below are the Phase-7 surface a Python user reaches via
//! `atomr.ddata.<name>`.

/// Names of every CRDT class exposed under `atomr.ddata`.
pub const CRDT_CLASSES: &[&str] = &[
    "GCounter",
    "PNCounter",
    "GSet",
    "ORSet",
    "LwwRegister",
    "Flag",
    "ORMap",
    "LWWMap",
    "PNCounterMap",
    "ORMultiMap",
];

/// Names of the replicator-related classes exposed under `atomr.ddata`.
pub const REPLICATOR_CLASSES: &[&str] = &[
    "Replicator",
    "ReplicatorSubscription",
    "ReadConsistency",
    "WriteConsistency",
    "DurableStore",
];

/// Names of the lower-level helpers exposed under `atomr.ddata`.
pub const HELPER_CLASSES: &[&str] =
    &["PruningState", "WriteAggregator", "ReadAggregator"];

/// Total number of Python classes exposed by Phase 7. Used by the
/// Python smoke test to guard against accidental class drops.
pub fn exposed_class_count() -> usize {
    CRDT_CLASSES.len() + REPLICATOR_CLASSES.len() + HELPER_CLASSES.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_lists_are_distinct() {
        for c in CRDT_CLASSES {
            assert!(!REPLICATOR_CLASSES.contains(c), "duplicate {c}");
            assert!(!HELPER_CLASSES.contains(c), "duplicate {c}");
        }
    }

    #[test]
    fn exposed_count_matches_lists() {
        assert_eq!(exposed_class_count(), 18);
    }
}
