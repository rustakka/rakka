//! Hosting submodule — fluent builder for Python apps.
//!
//! Rust-side builder is wrapped so Python can chain `.with_config`,
//! `.configure_interpreter`, `.on_start`, etc. and then `build()` to
//! receive an `ActorSystem`.

use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::actor_system::PyActorSystem;
use crate::config::PyConfig;
use crate::interpreter::InterpreterQuota;

#[pyclass(name = "ActorSystemBuilder", module = "rustakka._native.hosting")]
pub struct PyActorSystemBuilder {
    name: String,
    config: Mutex<Option<Py<PyConfig>>>,
    interpreter_pools: Mutex<Vec<(String, String, usize, Option<Py<InterpreterQuota>>)>>,
    on_start: Mutex<Vec<Py<PyAny>>>,
}

#[pymethods]
impl PyActorSystemBuilder {
    #[new]
    fn new(name: String) -> Self {
        Self {
            name,
            config: Mutex::new(None),
            interpreter_pools: Mutex::new(Vec::new()),
            on_start: Mutex::new(Vec::new()),
        }
    }

    fn with_config(slf: PyRefMut<'_, Self>, config: Py<PyConfig>) -> PyRefMut<'_, Self> {
        *slf.config.lock() = Some(config);
        slf
    }

    #[pyo3(signature = (label, dispatcher="python-pinned".to_string(), count=1, quota=None))]
    fn configure_interpreter(
        slf: PyRefMut<'_, Self>,
        label: String,
        dispatcher: String,
        count: usize,
        quota: Option<Py<InterpreterQuota>>,
    ) -> PyRefMut<'_, Self> {
        slf.interpreter_pools.lock().push((label, dispatcher, count, quota));
        slf
    }

    fn on_start(slf: PyRefMut<'_, Self>, callback: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.on_start.lock().push(callback);
        slf
    }

    fn build(&self, py: Python<'_>) -> PyResult<Py<PyActorSystem>> {
        let config = self.config.lock().as_ref().map(|c| c.clone_ref(py));
        let sys = PyActorSystem::create_blocking(py, self.name.clone(), config)?;
        {
            let pools = self.interpreter_pools.lock();
            for (label, dispatcher, count, quota) in pools.iter() {
                let sys_ref = sys.borrow(py);
                sys_ref.configure_interpreter(
                    label.clone(),
                    dispatcher.clone(),
                    *count,
                    quota.as_ref().map(|q| q.clone_ref(py)),
                )?;
            }
        }
        for cb in self.on_start.lock().iter() {
            cb.call1(py, (sys.clone_ref(py),))?;
        }
        Ok(sys)
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "hosting")?;
    sub.add_class::<PyActorSystemBuilder>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
