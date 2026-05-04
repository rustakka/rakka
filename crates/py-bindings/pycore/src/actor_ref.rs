//! `ActorRef` — untyped Python-facing handle. We always send
//! `Py<PyAny>` messages across the boundary; typed stubs live in the
//! Python facade.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use atomr_core::actor::ActorRef as RustRef;

use crate::py_actor::PyMessage;
use crate::runtime::runtime;

#[pyclass(name = "ActorRef", module = "atomr._native")]
pub struct PyActorRef {
    pub(crate) inner: Arc<RustRef<PyMessage>>,
    pub(crate) path: String,
}

impl PyActorRef {
    pub fn new(inner: RustRef<PyMessage>, path: String) -> Self {
        Self { inner: Arc::new(inner), path }
    }
}

#[pymethods]
impl PyActorRef {
    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Fire-and-forget send.
    fn tell(&self, msg: Bound<'_, PyAny>) -> PyResult<()> {
        let payload = msg.unbind();
        self.inner.tell(PyMessage::new(payload));
        Ok(())
    }

    /// Async ask — returns an `asyncio`-compatible awaitable.
    #[pyo3(signature = (msg, timeout=5.0))]
    fn ask<'py>(&self, py: Python<'py>, msg: Bound<'py, PyAny>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let payload = msg.unbind();
        let (env, rx) = PyMessage::ask(payload);
        self.inner.tell(env);
        let dur = std::time::Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match tokio::time::timeout(dur, rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => Err(PyErr::new::<crate::errors::AskError, _>("reply channel dropped")),
                Err(_) => Err(PyErr::new::<crate::errors::AskError, _>("ask timed out")),
            };
            match result {
                Ok(obj) => Ok(obj),
                Err(e) => Err(e),
            }
        })
    }

    /// Blocking ask, for sync CLI-style code. Spawns on the shared runtime.
    #[pyo3(signature = (msg, timeout=5.0))]
    fn ask_blocking(&self, py: Python<'_>, msg: Bound<'_, PyAny>, timeout: f64) -> PyResult<Py<PyAny>> {
        let payload = msg.unbind();
        let (env, rx) = PyMessage::ask(payload);
        self.inner.tell(env);
        let dur = std::time::Duration::from_secs_f64(timeout);
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                match tokio::time::timeout(dur, rx).await {
                    Ok(Ok(Ok(v))) => Ok(v),
                    Ok(Ok(Err(e))) => Err(e),
                    Ok(Err(_)) => Err(PyErr::new::<crate::errors::AskError, _>("reply channel dropped")),
                    Err(_) => Err(PyErr::new::<crate::errors::AskError, _>("ask timed out")),
                }
            })
        })
    }

    fn __repr__(&self) -> String {
        format!("<ActorRef path={}>", self.path)
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorRef>()
}
