//! DI submodule — type-keyed container that stores arbitrary Python objects.

use std::collections::HashMap;

use parking_lot::RwLock;
use pyo3::prelude::*;

#[pyclass(name = "ServiceContainer", module = "rustakka._native.di")]
pub struct PyServiceContainer {
    services: RwLock<HashMap<String, Py<PyAny>>>,
}

#[pymethods]
impl PyServiceContainer {
    #[new]
    fn new() -> Self { Self { services: RwLock::new(HashMap::new()) } }

    fn register(&self, key: String, value: Py<PyAny>) {
        self.services.write().insert(key, value);
    }

    fn resolve(&self, py: Python<'_>, key: String) -> Option<Py<PyAny>> {
        self.services.read().get(&key).map(|v| v.clone_ref(py))
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyList>> {
        let list = pyo3::types::PyList::empty_bound(py);
        for k in self.services.read().keys() { list.append(k)?; }
        Ok(list.unbind())
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "di")?;
    sub.add_class::<PyServiceContainer>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
