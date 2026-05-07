//! `atomr-pystreams` — helper façade for the Python streams binding.
//!
//! The actual `#[pyclass]` definitions live in `atomr-pycore`'s
//! `ext_streams` module so they can share the GIL-discipline newtype with
//! the rest of the bindings. This crate exists to mirror the Rust
//! workspace structure and to host pure-Rust helpers that are useful
//! both inside the bindings and to downstream consumers (e.g. pure-Rust
//! tests of the GIL-safe `Py<PyAny>` element type).

#![cfg_attr(not(test), allow(dead_code))]

/// Strategy for handling out-of-band errors raised inside Python
/// callbacks running on the materializer dispatcher. Mirrors
/// `atomr_streams::SupervisionDirective` for documentation purposes; the
/// actual decider compilation lives in `atomr-pycore`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CallbackErrorPolicy {
    /// Replace the offending element with `None` and continue.
    #[default]
    SubstituteNone,
    /// Drop the element silently.
    Drop,
    /// Stop the stream.
    Stop,
}

/// Suggested upper bound for `BroadcastHub` buffer size when no value is
/// supplied by the caller. The same default is used by the Python
/// facade.
pub const DEFAULT_HUB_BUFFER: usize = 16;

/// Suggested parallelism for `Flow.map_async` when not specified.
pub const DEFAULT_MAP_ASYNC_PARALLELISM: usize = 1;

#[cfg(test)]
mod tests {
    use super::*;

    const _: () = {
        assert!(DEFAULT_HUB_BUFFER >= 1);
        assert!(DEFAULT_MAP_ASYNC_PARALLELISM >= 1);
    };

    #[test]
    fn defaults_are_reasonable() {
        assert_eq!(CallbackErrorPolicy::default(), CallbackErrorPolicy::SubstituteNone);
    }
}
