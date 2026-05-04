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
        Self { factory, dispatcher, interpreter_role, mailbox }
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
        }
    }

    fn with_interpreter_role(&self, role: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: role,
            mailbox: self.mailbox.clone(),
        }
    }

    fn with_mailbox(&self, mailbox: String) -> Self {
        Self {
            factory: Python::with_gil(|py| self.factory.clone_ref(py)),
            dispatcher: self.dispatcher.clone(),
            interpreter_role: self.interpreter_role.clone(),
            mailbox: Some(mailbox),
        }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProps>()
}
