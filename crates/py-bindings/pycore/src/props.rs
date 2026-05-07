//! `Props` Python builder. Converts a Python callable factory into a
//! Rust `Props<PyActor>` at spawn time, applying dispatcher and
//! interpreter-pool routing.
//!
//! Phase 2 — Props now optionally carries a [`PySupervisorStrategy`]
//! that the spawn paths (`actor_system::actor_of` and
//! `py_actor::apply_op_eager` for `CtxOp::Spawn`) propagate into the
//! constructed `PyActor` and the underlying `Props<PyActor>`.

use pyo3::prelude::*;

use crate::supervision::PySupervisorStrategy;

#[pyclass(name = "Props", module = "atomr._native")]
pub struct PyProps {
    pub(crate) factory: Py<PyAny>,
    pub(crate) dispatcher: String,
    pub(crate) interpreter_role: String,
    pub(crate) mailbox: Option<String>,
    /// Optional supervisor strategy. `None` means "Rust default":
    /// `OneForOne` with restart-on-everything, 10 retries / 60s.
    pub(crate) supervisor_strategy: Option<PySupervisorStrategy>,
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

    fn with_dispatcher(&self, dispatcher: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher,
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
            supervisor_strategy: self.supervisor_strategy.clone(),
        }
    }

    fn with_interpreter_role(&self, role: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: role,
            mailbox: self.mailbox.clone(),
            supervisor_strategy: self.supervisor_strategy.clone(),
        }
    }

    fn with_mailbox(&self, mailbox: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: Some(mailbox),
            supervisor_strategy: self.supervisor_strategy.clone(),
        }
    }

    /// Attach a [`SupervisorStrategy`] to this `Props`. Children spawned
    /// from this Props inherit the strategy through the bound
    /// `PyActor`'s `supervisor_strategy()` impl.
    fn with_supervisor_strategy(&self, strategy: PySupervisorStrategy) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
            supervisor_strategy: Some(strategy),
        }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProps>()
}
