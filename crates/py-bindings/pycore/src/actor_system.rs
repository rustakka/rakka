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
use crate::ext_remote::{
    call_decoder, call_encoder, collision_to_pyerr, manifest_for, validate_manifest_with_mode,
    PyCodecRegistry,
};
use crate::interpreter::{InterpreterQuota, InterpreterRegistry};
use crate::props::PyProps;
use crate::py_actor::{PyActor, PyMessage};
use crate::runtime::runtime;

static REGISTRY: Lazy<InterpreterRegistry> = Lazy::new(InterpreterRegistry::default);

pub fn registry() -> &'static InterpreterRegistry {
    &REGISTRY
}

#[pyclass(name = "ActorSystem", module = "atomr._native")]
pub struct PyActorSystem {
    pub(crate) inner: RustSystem,
    pub(crate) codecs: PyCodecRegistry,
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
            Python::with_gil(|py| {
                Py::new(py, PyActorSystem { inner, codecs: PyCodecRegistry::default() })
                    .map(|p| p.into_any())
            })
        })
    }

    /// Sync (blocking) creation, convenient for top-level scripts.
    #[staticmethod]
    #[pyo3(signature = (name, config=None))]
    pub fn create_blocking(py: Python<'_>, name: String, config: Option<Py<PyConfig>>) -> PyResult<Py<Self>> {
        let cfg = config.map(|c| c.borrow(py).inner.clone()).unwrap_or_else(atomr_config::Config::empty);
        let rt = runtime();
        let inner = py.allow_threads(|| rt.block_on(RustSystem::create(name, cfg))).map_err(errors::map)?;
        Py::new(py, PyActorSystem { inner, codecs: PyCodecRegistry::default() })
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
        use crate::props::PropsKind;
        let props_ref = props.borrow(py);
        let kind_clone = props_ref.kind.clone();
        let factory = props_ref.factory.clone_ref(py);
        let dispatcher_name = props_ref.dispatcher.clone();
        let role = props_ref.interpreter_role.clone();
        let strategy: SupervisorStrategy = props_ref
            .supervisor_strategy
            .as_ref()
            .map(|s| s.rust().clone())
            .unwrap_or_default();
        drop(props_ref);

        let rt = runtime();
        let system = self.inner.clone();
        let name_cl = name.clone();

        let actor_ref = match kind_clone {
            PropsKind::Python => {
                let kind = dispatcher::parse(&dispatcher_name, 1);
                let pool = REGISTRY.get_or_create(&role, kind, InterpreterQuota::default());
                pool.register_actor()?;

                let hash_seed = stable_hash(&format!("{}/{}", self.inner.name(), name));
                let factory_for_actor = factory;
                let pool_cl = pool.clone();
                let strategy_for_actor = strategy.clone();

                let rust_props = RustProps::<PyActor>::create(move || {
                    let factory = Python::with_gil(|py| factory_for_actor.clone_ref(py));
                    PyActor::new(factory, pool_cl.clone(), hash_seed, strategy_for_actor.clone())
                })
                .with_supervisor_strategy(strategy.clone());

                py.allow_threads(|| {
                    let _guard = rt.enter();
                    system.actor_of(rust_props, &name_cl)
                })
                .map_err(errors::map)?
            }
            PropsKind::Router { logic, n, child_props } => {
                let cp = child_props.clone();
                let rust_props = RustProps::<crate::ext_routing::RouterActor>::create(move || {
                    crate::ext_routing::RouterActor::new(cp.clone(), n, logic)
                });
                py.allow_threads(|| {
                    let _guard = rt.enter();
                    system.actor_of(rust_props, &name_cl)
                })
                .map_err(errors::map)?
            }
            PropsKind::Backoff { child_props, min, max, random_factor } => {
                let opts = atomr_core::pattern::BackoffOptions {
                    min_backoff: min,
                    max_backoff: max,
                    random_factor,
                    max_restarts: None,
                };
                let cp = child_props.clone();
                let rust_props = RustProps::<crate::ext_routing::BackoffActor>::create(move || {
                    crate::ext_routing::BackoffActor::new(cp.clone(), opts.clone())
                });
                py.allow_threads(|| {
                    let _guard = rt.enter();
                    system.actor_of(rust_props, &name_cl)
                })
                .map_err(errors::map)?
            }
        };
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

    /// Phase 9 — codec registry access.
    #[getter]
    fn codecs(&self) -> PyCodecRegistry {
        self.codecs.clone()
    }

    /// Register a codec for one or more `module.qualname` manifests.
    ///
    /// Each manifest is validated. Strict mode (the default) imports
    /// the module and walks the qualname; failures raise `ValueError`.
    /// Pass `strict=False` to skip the importlib round-trip — handy
    /// for `__main__`-scoped classes and inline test fixtures. Lax
    /// validation emits `warnings.warn(...)` per manifest so production
    /// code does not silently rely on it.
    ///
    /// On collision (manifest already registered) and `force=False`,
    /// raises `ValueError` listing the existing codec name. With
    /// `force=True`, the existing entry is silently replaced.
    #[pyo3(signature = (name, encoder, decoder, manifests, force=false, strict=true))]
    fn register_codec(
        &self,
        py: Python<'_>,
        name: String,
        encoder: Py<PyAny>,
        decoder: Py<PyAny>,
        manifests: Vec<String>,
        force: bool,
        strict: bool,
    ) -> PyResult<()> {
        if manifests.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "register_codec: manifests must not be empty",
            ));
        }
        for m in &manifests {
            validate_manifest_with_mode(py, m, strict)?;
        }
        self.codecs
            .insert(name, encoder, decoder, &manifests, force)
            .map_err(collision_to_pyerr)
    }

    /// Convenience: install the JSON codec for `manifests` (or none —
    /// pair with `default=True` to fall back for any unmatched
    /// manifest).
    ///
    /// Honors the same `force` and `strict` flags as `register_codec`.
    /// On collision and `force=False`, raises `ValueError`.
    #[pyo3(signature = (manifests=Vec::new(), default=false, force=false, strict=true))]
    fn use_json_codec(
        &self,
        py: Python<'_>,
        manifests: Vec<String>,
        default: bool,
        force: bool,
        strict: bool,
    ) -> PyResult<()> {
        for m in &manifests {
            validate_manifest_with_mode(py, m, strict)?;
        }
        if !manifests.is_empty() {
            self.codecs
                .install_json(py, &manifests, force)?
                .map_err(collision_to_pyerr)?;
        }
        if default {
            let (encoder, decoder) = crate::ext_remote::build_json_pair(py)?;
            self.codecs.install_default(encoder, decoder);
        }
        Ok(())
    }

    /// In-process "remote" send: encode `obj` via the registered codec
    /// (manifest derived from the object's class), decode on the other
    /// side, then `tell` the result on `target`. This exercises the
    /// full Phase-9 codec pipeline without crossing a network — the
    /// wire-level path through `atomr-remote` reuses the same encode
    /// / decode functions.
    fn tell_remote(
        &self,
        py: Python<'_>,
        target: Py<PyActorRef>,
        msg: Py<PyAny>,
    ) -> PyResult<()> {
        let manifest = manifest_for(py, &msg)?;
        let (encoder, decoder) = self.codecs.lookup(&manifest).ok_or_else(|| {
            PyErr::new::<errors::AtomrError, _>(format!(
                "no codec registered for manifest `{manifest}`"
            ))
        })?;
        let bytes = call_encoder(py, &encoder, &msg)?;
        let decoded = call_decoder(py, &decoder, &bytes)?;
        let target_ref = target.borrow(py);
        target_ref.inner.tell(PyMessage::new(decoded));
        Ok(())
    }

    /// Round-trip `obj` through the codec registry without sending.
    /// Useful for tests of the codec wiring and for debugging the
    /// manifest derivation.
    fn codec_roundtrip<'py>(
        &self,
        py: Python<'py>,
        msg: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let manifest = manifest_for(py, &msg)?;
        let (encoder, decoder) = self.codecs.lookup(&manifest).ok_or_else(|| {
            PyErr::new::<errors::AtomrError, _>(format!(
                "no codec registered for manifest `{manifest}`"
            ))
        })?;
        let bytes = call_encoder(py, &encoder, &msg)?;
        let decoded = call_decoder(py, &decoder, &bytes)?;
        Ok(decoded.into_bound(py))
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
