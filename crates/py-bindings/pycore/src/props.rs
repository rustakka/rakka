//! `Props` Python builder. Converts a Python callable factory into a
//! Rust `Props<PyActor>` at spawn time, applying dispatcher and
//! interpreter-pool routing.
//!
//! Phase 2 — Props optionally carries a [`PySupervisorStrategy`] that
//! the spawn paths (`actor_system::actor_of` and
//! `py_actor::apply_op_eager` for `CtxOp::Spawn`) propagate into the
//! constructed `PyActor` and the underlying `Props<PyActor>`.
//!
//! Phase 3 — Props additionally carries a `kind: PropsKind` tag that
//! selects between standard Python actors, routers, and backoff
//! supervisors. Router constructors live in [`crate::ext_routing`].
//!
//! Round-2 Epic B — atomr-core's actor_cell now enforces
//! `max_retries`/`within_seconds` on the `SupervisorStrategy`. The
//! existing `with_supervisor_strategy(...)` builder already plumbs
//! those fields through `PySupervisorStrategy`; Epic B's enforcement
//! is transparent at this layer.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use crate::supervision::PySupervisorStrategy;

/// What kind of native actor this `Props` produces.
#[derive(Clone)]
pub enum PropsKind {
    /// Standard Python actor — `factory()` constructs a new Python
    /// `Actor` instance per (re)start.
    Python,
    /// Router that owns N children built from `child_props` and routes
    /// `PyMessage` according to `logic`.
    Router { logic: RoutingLogic, n: usize, child_props: Arc<PyProps> },
    /// Backoff supervisor: holds a single child built from
    /// `child_props` and restarts it with exponential backoff.
    Backoff { child_props: Arc<PyProps>, min: Duration, max: Duration, random_factor: f64 },
}

/// Routing logic selector — see [`crate::ext_routing`].
#[derive(Clone, Copy, Debug)]
pub enum RoutingLogic {
    Broadcast,
    RoundRobin,
    Random,
    ConsistentHash,
    SmallestMailbox,
    TailChopping { interval_secs: f64, within_secs: f64 },
    ScatterGather { within_secs: f64 },
}

#[pyclass(name = "Props", module = "atomr._native")]
pub struct PyProps {
    pub(crate) factory: Py<PyAny>,
    pub(crate) dispatcher: String,
    pub(crate) interpreter_role: String,
    pub(crate) mailbox: Option<String>,
    /// Optional supervisor strategy. `None` means "Rust default":
    /// `OneForOne` with restart-on-everything, 10 retries / 60s.
    pub(crate) supervisor_strategy: Option<PySupervisorStrategy>,
    pub(crate) kind: PropsKind,
}

impl Clone for PyProps {
    fn clone(&self) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
            supervisor_strategy: self.supervisor_strategy.clone(),
            kind: self.kind.clone(),
        }
    }
}

#[pymethods]
impl PyProps {
    #[staticmethod]
    #[pyo3(signature = (factory, dispatcher="python-pinned".to_string(), interpreter_role="default".to_string(), mailbox=None))]
    fn create(
        factory: Py<PyAny>,
        dispatcher: String,
        interpreter_role: String,
        mailbox: Option<String>,
    ) -> Self {
        Self {
            factory,
            dispatcher,
            interpreter_role,
            mailbox,
            supervisor_strategy: None,
            kind: PropsKind::Python,
        }
    }

    #[getter]
    fn dispatcher(&self) -> &str {
        &self.dispatcher
    }

    #[getter]
    fn interpreter_role(&self) -> &str {
        &self.interpreter_role
    }

    #[getter]
    fn supervisor_strategy(&self) -> Option<PySupervisorStrategy> {
        self.supervisor_strategy.clone()
    }

    /// Internal hint — exposed as a string label for diagnostics.
    #[getter]
    fn kind_label(&self) -> &'static str {
        match &self.kind {
            PropsKind::Python => "python",
            PropsKind::Router { .. } => "router",
            PropsKind::Backoff { .. } => "backoff",
        }
    }

    fn with_dispatcher(&self, dispatcher: String) -> Self {
        let mut c = self.clone();
        c.dispatcher = dispatcher;
        c
    }

    fn with_interpreter_role(&self, role: String) -> Self {
        let mut c = self.clone();
        c.interpreter_role = role;
        c
    }

    fn with_mailbox(&self, mailbox: String) -> Self {
        let mut c = self.clone();
        c.mailbox = Some(mailbox);
        c
    }

    /// Attach a [`SupervisorStrategy`] to this `Props`. Children spawned
    /// from this Props inherit the strategy through the bound
    /// `PyActor`'s `supervisor_strategy()` impl.
    fn with_supervisor_strategy(&self, strategy: PySupervisorStrategy) -> Self {
        let mut c = self.clone();
        c.supervisor_strategy = Some(strategy);
        c
    }

    /// Convenience: produce a Props whose supervisor strategy uses the
    /// given retry budget. Equivalent to
    /// `with_supervisor_strategy(PySupervisorStrategy::one_for_one(...,
    /// max_retries=N, within_seconds=W))` but with a default decider
    /// that restarts on any exception.
    #[pyo3(signature = (max_retries, within_seconds=60.0))]
    fn with_supervisor_budget(&self, max_retries: u32, within_seconds: f64) -> Self {
        let strategy = PySupervisorStrategy::default_with_budget(max_retries, within_seconds);
        let mut c = self.clone();
        c.supervisor_strategy = Some(strategy);
        c
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProps>()
}
