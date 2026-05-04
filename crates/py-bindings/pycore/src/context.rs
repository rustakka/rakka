//! `Context` Python shim. The Rust `Context<A>` is not thread-safe, so we
//! don't hand it to Python directly. Instead, each Python call receives a
//! lightweight `Context` object populated with the bits the user cares
//! about (self_ref, path, sender); spawn/watch/stash are proxied back
//! through the Rust cell via command channels.
//!
//! For the first shipping slice we expose read-only accessors + `stop_self`
//! and `spawn_child`; `stash/unstash_all/watch/unwatch/set_receive_timeout`
//! are tracked in PORTING_TODO under Phase P1 follow-ups.

use pyo3::prelude::*;

use crate::actor_ref::PyActorRef;

#[pyclass(name = "Context", module = "atomr._native")]
pub struct PyContext {
    pub(crate) self_ref: Py<PyActorRef>,
    pub(crate) path: String,
}

#[pymethods]
impl PyContext {
    #[getter]
    fn self_ref(&self, py: Python<'_>) -> Py<PyActorRef> {
        self.self_ref.clone_ref(py)
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()
}
