//! Process Manager pattern — typed state-machine alternative to
//! [`crate::saga::SagaPattern`].
//!
//! Where [`crate::saga::Saga`] is free-form (mutate state, emit
//! actions), a [`ProcessManager`] is bounded: every event causes a
//! [`Transition`] — `Stay`, move to a new `State` and dispatch
//! commands, or `Complete`. Use it when the state space is small and
//! enumerable, and you want compile-time exhaustiveness checking on
//! handle clauses.

mod runner;

pub use runner::{
    ProcessManager, ProcessManagerBuilder, ProcessManagerHandles, ProcessManagerPattern,
    ProcessManagerTopology, Transition,
};
