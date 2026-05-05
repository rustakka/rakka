//! Cluster-tools submodule: DistributedPubSub, ClusterClient, Singleton.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;

use atomr_cluster_tools::{
    ClusterClientSettings, ClusterReceptionist, ClusterSingletonManager, SingletonState,
};

#[pyclass(name = "DistributedPubSub", module = "atomr._native.cluster_tools")]
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

/// Tracks cluster-singleton state independently of actor-ref binding.
///
/// PyO3 limitation: `UntypedActorRef` doesn't round-trip through Python
/// without a fully-typed actor binding (the inner ref carries type info
/// erased through `Box<dyn Any>` which isn't usable from Python). The
/// Python view exposes the state-machine surface — Inactive / Starting
/// / HandingOver / Active — and the buffered/drops counters; live
/// delivery still flows through Rust if the manager is shared via a
/// Rust-side cluster instance.
#[pyclass(name = "ClusterSingletonManager", module = "atomr._native.cluster_tools")]
pub struct PyClusterSingletonManager {
    inner: Arc<ClusterSingletonManager>,
}

#[pymethods]
impl PyClusterSingletonManager {
    #[new]
    #[pyo3(signature = (buffer_size=1000))]
    fn new(buffer_size: usize) -> Self {
        Self { inner: ClusterSingletonManager::with_buffer_size(buffer_size) }
    }

    /// Current state name: `inactive`, `starting`, `handing_over`,
    /// `active_here`, `active_remote`.
    #[getter]
    fn state(&self) -> String {
        match self.inner.state() {
            SingletonState::Inactive => "inactive".into(),
            SingletonState::Starting => "starting".into(),
            SingletonState::HandingOver => "handing_over".into(),
            SingletonState::Active { here: true, .. } => "active_here".into(),
            SingletonState::Active { here: false, .. } => "active_remote".into(),
            _ => "unknown".into(),
        }
    }

    fn begin_handover(&self) {
        self.inner.begin_handover();
    }
    fn begin_starting(&self) {
        self.inner.begin_starting();
    }
    fn clear(&self) {
        self.inner.clear();
    }

    #[getter]
    fn buffered(&self) -> usize {
        self.inner.buffered()
    }
    #[getter]
    fn drops(&self) -> u64 {
        self.inner.drops()
    }
}

/// Server-side registry mapping logical names to actor refs.
#[pyclass(name = "ClusterReceptionist", module = "atomr._native.cluster_tools")]
pub struct PyClusterReceptionist {
    inner: Arc<ClusterReceptionist>,
}

#[pymethods]
impl PyClusterReceptionist {
    #[new]
    fn new() -> Self {
        Self { inner: ClusterReceptionist::new() }
    }

    /// Names of services registered with this receptionist.
    fn registered(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for n in self.inner.registered() {
            list.append(n)?;
        }
        Ok(list.unbind())
    }

    /// Drop the named service.
    fn unregister(&self, name: String) {
        self.inner.unregister(&name);
    }

    /// True if `name` resolves to a registered actor ref.
    fn has(&self, name: String) -> bool {
        self.inner.lookup(&name).is_some()
    }
}

/// Client-side proxy settings (initial-contact list, retry limit).
#[pyclass(name = "ClusterClientSettings", module = "atomr._native.cluster_tools")]
#[derive(Clone)]
pub struct PyClusterClientSettings {
    pub(crate) inner: ClusterClientSettings,
}

#[pymethods]
impl PyClusterClientSettings {
    #[new]
    #[pyo3(signature = (initial_contacts=Vec::new(), max_attempts=5))]
    fn new(initial_contacts: Vec<String>, max_attempts: u32) -> Self {
        Self {
            inner: ClusterClientSettings::default()
                .with_initial_contacts(initial_contacts)
                .with_max_attempts(max_attempts),
        }
    }

    fn with_initial_contacts(&self, contacts: Vec<String>) -> Self {
        Self { inner: self.inner.clone().with_initial_contacts(contacts) }
    }

    fn with_max_attempts(&self, n: u32) -> Self {
        Self { inner: self.inner.clone().with_max_attempts(n) }
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster_tools")?;
    sub.add_class::<PyDistributedPubSub>()?;
    sub.add_class::<PyClusterSingletonManager>()?;
    sub.add_class::<PyClusterReceptionist>()?;
    sub.add_class::<PyClusterClientSettings>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
