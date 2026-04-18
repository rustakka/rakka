//! Coordination submodule — lease primitives.

use std::sync::Arc;

use pyo3::prelude::*;

use rustakka_coordination::{InMemoryLease, Lease};

use crate::errors;
use crate::runtime::runtime;

#[pyclass(name = "InMemoryLease", module = "rustakka._native.coordination")]
pub struct PyInMemoryLease { inner: Arc<InMemoryLease> }

#[pymethods]
impl PyInMemoryLease {
    #[new]
    fn new() -> Self { Self { inner: InMemoryLease::new() } }

    fn acquire(&self, py: Python<'_>, owner: String) -> PyResult<bool> {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| rt.block_on(async move {
            inner.acquire(&owner).await.map_err(errors::map)
        }))
    }

    fn release(&self, py: Python<'_>, owner: String) -> PyResult<()> {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| rt.block_on(async move {
            inner.release(&owner).await.map_err(errors::map)
        }))
    }

    fn check(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let inner = self.inner.clone();
        let rt = runtime();
        Ok(py.allow_threads(|| rt.block_on(async move { inner.check_lease().await })))
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "coordination")?;
    sub.add_class::<PyInMemoryLease>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
