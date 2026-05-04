//! Cluster-sharding submodule (Phase P6 slice).
//!
//! We expose a simple `Shard` + `ShardRegion` facade that routes by
//! `entity_id` using a Python-supplied extractor callable.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;

#[pyclass(name = "ShardRegion", module = "atomr._native.cluster_sharding")]
pub struct PyShardRegion {
    entity_factory: Py<PyAny>,
    extractor: Py<PyAny>,
    entities: Arc<Mutex<HashMap<String, Py<PyAny>>>>,
}

#[pymethods]
impl PyShardRegion {
    #[new]
    fn new(entity_factory: Py<PyAny>, extractor: Py<PyAny>) -> Self {
        Self { entity_factory, extractor, entities: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn deliver(&self, py: Python<'_>, message: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let ext = self.extractor.bind(py);
        let tuple = ext.call1((message.clone_ref(py),))?;
        let (entity_id, payload): (String, Py<PyAny>) = tuple.extract()?;
        let entity = {
            let mut g = self.entities.lock();
            if let Some(e) = g.get(&entity_id) {
                e.clone_ref(py)
            } else {
                let e: Py<PyAny> = self.entity_factory.call1(py, (entity_id.clone(),))?;
                g.insert(entity_id.clone(), e.clone_ref(py));
                e
            }
        };
        let result = entity.call_method1(py, "handle", (payload,))?;
        Ok(result)
    }

    fn entity_count(&self) -> usize {
        self.entities.lock().len()
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster_sharding")?;
    sub.add_class::<PyShardRegion>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
