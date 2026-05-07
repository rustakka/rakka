//! `Props` Python builder. Converts a Python callable factory into a
//! Rust `Props<PyActor>` at spawn time, applying dispatcher and
//! interpreter-pool routing.
//!
//! Phase 3 extension: `PyProps` carries an optional `router` variant.
//! When present, `actor_of` spawns a `RouterActor<L>` that owns N
//! children and forwards `PyMessage`s according to the routing logic.
//! Router constructors live in [`crate::ext_routing`].

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

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
    pub(crate) kind: PropsKind,
}

impl Clone for PyProps {
    fn clone(&self) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
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
        Self { factory, dispatcher, interpreter_role, mailbox, kind: PropsKind::Python }
    }

    #[getter]
    fn dispatcher(&self) -> &str {
        &self.dispatcher
    }

    #[getter]
    fn interpreter_role(&self) -> &str {
        &self.interpreter_role
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
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProps>()
}
