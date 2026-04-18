//! Discovery submodule.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use rustakka_discovery::{ResolvedTarget, ServiceDiscovery, StaticDiscovery};

use crate::runtime::runtime;

#[pyclass(name = "StaticDiscovery", module = "rustakka._native.discovery")]
pub struct PyStaticDiscovery { inner: Arc<StaticDiscovery> }

#[pymethods]
impl PyStaticDiscovery {
    #[new]
    fn new() -> Self { Self { inner: StaticDiscovery::new() } }

    #[pyo3(signature = (service, host, port=None))]
    fn register(&self, service: String, host: String, port: Option<u16>) {
        self.inner.register(service, ResolvedTarget { host, port });
    }

    fn lookup<'py>(&self, py: Python<'py>, service: String) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let resolved = py.allow_threads(|| rt.block_on(inner.lookup(&service)));
        let list = PyList::empty_bound(py);
        for t in resolved.addresses {
            let tup: Py<PyAny> = (t.host, t.port).into_py(py);
            list.append(tup)?;
        }
        Ok(list)
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "discovery")?;
    sub.add_class::<PyStaticDiscovery>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
