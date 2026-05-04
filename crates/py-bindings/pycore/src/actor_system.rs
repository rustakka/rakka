//! `ActorSystem` Python binding.

use std::hash::{Hash, Hasher};

use once_cell::sync::Lazy;
use pyo3::prelude::*;

use atomr_core::actor::{ActorSystem as RustSystem, Props as RustProps};
use atomr_core::supervision::SupervisorStrategy;

use crate::actor_ref::PyActorRef;
use crate::config::PyConfig;
use crate::dispatcher;
use crate::errors;
use crate::interpreter::{InterpreterQuota, InterpreterRegistry};
use crate::props::PyProps;
use crate::py_actor::PyActor;
use crate::runtime::runtime;

static REGISTRY: Lazy<InterpreterRegistry> = Lazy::new(InterpreterRegistry::default);

pub fn registry() -> &'static InterpreterRegistry {
    &REGISTRY
}

#[pyclass(name = "ActorSystem", module = "atomr._native")]
pub struct PyActorSystem {
    pub(crate) inner: RustSystem,
}

#[pymethods]
impl PyActorSystem {
    #[staticmethod]
    #[pyo3(signature = (name, config=None))]
    fn create<'py>(
        py: Python<'py>,
        name: String,
        config: Option<Py<PyConfig>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cfg = config
            .map(|c| Python::with_gil(|py| c.borrow(py).inner.clone()))
            .unwrap_or_else(atomr_config::Config::empty);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = RustSystem::create(name, cfg).await.map_err(errors::map)?;
            Python::with_gil(|py| Py::new(py, PyActorSystem { inner }).map(|p| p.into_any()))
        })
    }

    /// Sync (blocking) creation, convenient for top-level scripts.
    #[staticmethod]
    #[pyo3(signature = (name, config=None))]
    pub fn create_blocking(py: Python<'_>, name: String, config: Option<Py<PyConfig>>) -> PyResult<Py<Self>> {
        let cfg = config.map(|c| c.borrow(py).inner.clone()).unwrap_or_else(atomr_config::Config::empty);
        let rt = runtime();
        let inner = py.allow_threads(|| rt.block_on(RustSystem::create(name, cfg))).map_err(errors::map)?;
        Py::new(py, PyActorSystem { inner })
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    /// Create or reuse an interpreter pool.
    #[pyo3(signature = (label, dispatcher="python-pinned".to_string(), count=1, quota=None))]
    pub fn configure_interpreter(
        &self,
        label: String,
        dispatcher: String,
        count: usize,
        quota: Option<Py<InterpreterQuota>>,
    ) -> PyResult<()> {
        let kind = dispatcher::parse(&dispatcher, count);
        let quota = quota.map(|q| Python::with_gil(|py| q.borrow(py).clone())).unwrap_or_default();
        REGISTRY.get_or_create(&label, kind, quota);
        Ok(())
    }

    /// Spawn a Python actor under `/user`.
    fn actor_of(&self, py: Python<'_>, props: Py<PyProps>, name: String) -> PyResult<Py<PyActorRef>> {
        let props_ref = props.borrow(py);
        let factory = props_ref.factory.clone_ref(py);
        let dispatcher_name = props_ref.dispatcher.clone();
        let role = props_ref.interpreter_role.clone();
        drop(props_ref);

        let kind = dispatcher::parse(&dispatcher_name, 1);
        let pool = REGISTRY.get_or_create(&role, kind, InterpreterQuota::default());
        pool.register_actor()?;

        let hash_seed = stable_hash(&format!("{}/{}", self.inner.name(), name));
        let strategy = SupervisorStrategy::default();
        let factory_for_actor = factory;
        let pool_cl = pool.clone();

        let rust_props = RustProps::<PyActor>::create(move || {
            let factory = Python::with_gil(|py| factory_for_actor.clone_ref(py));
            PyActor::new(factory, pool_cl.clone(), hash_seed, strategy.clone())
        });

        let rt = runtime();
        let system = self.inner.clone();
        let name_cl = name.clone();
        let actor_ref = py
            .allow_threads(|| {
                let _guard = rt.enter();
                system.actor_of(rust_props, &name_cl)
            })
            .map_err(errors::map)?;
        let path = format!("akka://{}/user/{}", self.inner.name(), name);
        Py::new(py, PyActorRef::new(actor_ref, path))
    }

    /// Async terminate.
    fn terminate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.terminate().await;
            Ok(())
        })
    }

    fn terminate_blocking(&self, py: Python<'_>) {
        let rt = runtime();
        let inner = self.inner.clone();
        py.allow_threads(|| rt.block_on(async move { inner.terminate().await }));
    }

    fn when_terminated<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.when_terminated().await;
            Ok(())
        })
    }
}

fn stable_hash(s: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorSystem>()
}
