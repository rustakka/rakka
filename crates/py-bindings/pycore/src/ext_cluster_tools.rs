//! Cluster-tools submodule: DistributedPubSub, ClusterClient, Singleton.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyclass(name = "DistributedPubSub", module = "rakka._native.cluster_tools")]
pub struct PyDistributedPubSub {
    topics: Arc<Mutex<std::collections::HashMap<String, Vec<Py<PyAny>>>>>,
}

#[pymethods]
impl PyDistributedPubSub {
    #[new]
    fn new() -> Self {
        Self { topics: Arc::new(Mutex::new(Default::default())) }
    }

    fn subscribe(&self, topic: String, callback: Py<PyAny>) {
        self.topics.lock().entry(topic).or_default().push(callback);
    }

    fn publish(&self, py: Python<'_>, topic: String, message: Py<PyAny>) -> PyResult<()> {
        let subs: Vec<Py<PyAny>> = {
            let g = self.topics.lock();
            g.get(&topic).map(|v| v.iter().map(|c| c.clone_ref(py)).collect()).unwrap_or_default()
        };
        for cb in subs {
            cb.call1(py, (message.clone_ref(py),))?;
        }
        Ok(())
    }

    fn topics(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for k in self.topics.lock().keys() {
            list.append(k)?;
        }
        Ok(list.unbind())
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster_tools")?;
    sub.add_class::<PyDistributedPubSub>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
