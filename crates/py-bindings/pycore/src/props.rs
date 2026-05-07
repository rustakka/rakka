//! `Props` Python builder. Converts a Python callable factory into a
//! Rust `Props<PyActor>` at spawn time, applying dispatcher and
//! interpreter-pool routing.

use pyo3::prelude::*;

#[pyclass(name = "Props", module = "atomr._native")]
pub struct PyProps {
    pub(crate) factory: Py<PyAny>,
    pub(crate) dispatcher: String,
    pub(crate) interpreter_role: String,
    pub(crate) mailbox: Option<String>,
    /// Optional supervisor budget: `(max_retries, within_seconds)`. When
    /// `None`, the default `OneForOneStrategy` is used (max 10 / 60s).
    pub(crate) supervisor_budget: Option<(u32, f64)>,
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
        Self { factory, dispatcher, interpreter_role, mailbox, supervisor_budget: None }
    }

    #[getter]
    fn dispatcher(&self) -> &str {
        &self.dispatcher
    }

    #[getter]
    fn interpreter_role(&self) -> &str {
        &self.interpreter_role
    }

    fn with_dispatcher(&self, dispatcher: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher,
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
            supervisor_budget: self.supervisor_budget,
        }
    }

    fn with_interpreter_role(&self, role: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: role,
            mailbox: self.mailbox.clone(),
            supervisor_budget: self.supervisor_budget,
        }
    }

    fn with_mailbox(&self, mailbox: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: Some(mailbox),
            supervisor_budget: self.supervisor_budget,
        }
    }

    /// Override the supervisor strategy's `(max_retries, within_seconds)`
    /// budget. If the actor's restart history within `within_seconds`
    /// reaches `max_retries`, the next failure escalates (currently
    /// stops the actor).
    #[pyo3(signature = (max_retries, within_seconds=60.0))]
    fn with_supervisor_budget(&self, max_retries: u32, within_seconds: f64) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: self.mailbox.clone(),
            supervisor_budget: Some((max_retries, within_seconds)),
        }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProps>()
}
