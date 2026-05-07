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

    /// Construct from a pre-shared Arc — avoids cloning the underlying
    /// `RustRef` when we already have it `Arc`-wrapped (used by the
    /// Phase 1 context plumbing where the same ref is exposed to
    /// Python multiple times per dispatch).
    pub fn from_arc(inner: Arc<RustRef<PyMessage>>, path: String) -> Self {
        Self { inner, path }
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

    /// Fire-and-forget send with explicit `sender`. The receiver's
    /// `ctx.sender` will resolve to `sender`.
    fn tell_with_sender(&self, msg: Bound<'_, PyAny>, sender: Py<PyActorRef>) -> PyResult<()> {
        let payload = msg.unbind();
        let sender_inner = Python::with_gil(|py| sender.borrow(py).inner.clone());
        self.inner.tell(PyMessage::with_sender(payload, sender_inner));
        Ok(())
    }

    /// Fire-and-forget send with an explicit consistent-hash routing
    /// key. Required when sending through a `Props.consistent_hash`
    /// router; otherwise the router has no stable basis for picking a
    /// routee.
    fn tell_with_key(&self, msg: Bound<'_, PyAny>, key: u64) -> PyResult<()> {
        let payload = msg.unbind();
        self.inner.tell(PyMessage::with_hash(payload, key));
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

    /// Send a `SystemMsg::Stop` to the target. The actor finishes the
    /// current message (if any), runs `post_stop`, and notifies any
    /// watchers via `Terminated`.
    fn stop(&self) {
        self.inner.stop();
    }

    /// Best-effort: returns `True` once the actor cell has shut down.
    /// For remote refs we cannot inspect the far-end mailbox so this
    /// always returns `False`.
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }

    fn __repr__(&self) -> String {
        format!("<ActorRef path={}>", self.path)
    }

    /// Return a sibling `ActorRef` with the same underlying mailbox
    /// channel but a rewritten path. Used by Epic A's remote-tell
    /// tests to mint a "remote-shaped" ref pointing at another
    /// system's TCP-resolved address — `tell_remote` consults the
    /// path string when deciding local-vs-remote routing.
    ///
    /// The `inner` channel is only relevant for the local fast-path;
    /// for true remote sends the transport delivers via path lookup
    /// on the receiving side, so the original `inner` is harmless.
    fn with_path(slf: PyRef<'_, Self>, path: String) -> PyActorRef {
        PyActorRef { inner: slf.inner.clone(), path }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorRef>()
}
