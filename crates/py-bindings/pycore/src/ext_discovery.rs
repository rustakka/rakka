//! Discovery submodule.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;

use atomr_discovery::{AggregateDiscovery, ResolvedTarget, ServiceDiscovery, StaticDiscovery};

use crate::runtime::runtime;

#[pyclass(name = "StaticDiscovery", module = "atomr._native.discovery")]
pub struct PyStaticDiscovery {
    pub(crate) inner: Arc<StaticDiscovery>,
}

#[pymethods]
impl PyStaticDiscovery {
    #[new]
    fn new() -> Self {
        Self { inner: StaticDiscovery::new() }
    }

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

/// Chain-of-responsibility discovery — walks providers in order and
/// returns the first non-empty resolution. akka.net:
/// `Akka.Discovery.Aggregate.AggregateServiceDiscovery`.
#[pyclass(name = "AggregateDiscovery", module = "atomr._native.discovery")]
pub struct PyAggregateDiscovery {
    inner: Arc<AggregateDiscovery>,
}

#[pymethods]
impl PyAggregateDiscovery {
    #[new]
    fn new(providers: Vec<PyRef<'_, PyStaticDiscovery>>) -> Self {
        let inner = AggregateDiscovery::new(
            providers
                .into_iter()
                .map(|p| p.inner.clone() as Arc<dyn ServiceDiscovery>)
                .collect(),
        );
        Self { inner }
    }

    /// Number of providers in this chain.
    fn provider_count(&self) -> usize {
        self.inner.provider_count()
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
    sub.add_class::<PyAggregateDiscovery>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
