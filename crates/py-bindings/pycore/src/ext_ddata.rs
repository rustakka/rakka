//! Distributed Data CRDTs exposed to Python. Uses the Rust crate's
//! native types directly for deterministic merge semantics.

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;

use atomr_distributed_data::{
    CrdtMerge, GCounter, GSet, OrSet, PNCounter, PruningPhase, PruningState, ReadAggregator,
    WriteAggregator,
};

#[pyclass(name = "GCounter", module = "atomr._native.ddata")]
pub struct PyGCounter {
    inner: Mutex<GCounter>,
}

#[pymethods]
impl PyGCounter {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(GCounter::new()) }
    }
    fn increment(&self, node: String, delta: u64) {
        self.inner.lock().increment(&node, delta);
    }
    fn value(&self) -> u64 {
        self.inner.lock().value()
    }
    fn merge(&self, other: &PyGCounter) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

#[pyclass(name = "PNCounter", module = "atomr._native.ddata")]
pub struct PyPNCounter {
    inner: Mutex<PNCounter>,
}

#[pymethods]
impl PyPNCounter {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(PNCounter::new()) }
    }
    fn increment(&self, node: String, delta: u64) {
        self.inner.lock().increment(&node, delta);
    }
    fn decrement(&self, node: String, delta: u64) {
        self.inner.lock().decrement(&node, delta);
    }
    fn value(&self) -> i64 {
        self.inner.lock().value()
    }
    fn merge(&self, other: &PyPNCounter) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

#[pyclass(name = "GSet", module = "atomr._native.ddata")]
pub struct PyGSet {
    inner: Mutex<GSet<String>>,
}

#[pymethods]
impl PyGSet {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(GSet::new()) }
    }
    fn add(&self, item: String) {
        self.inner.lock().add(item);
    }
    fn contains(&self, item: String) -> bool {
        self.inner.lock().contains(&item)
    }
    fn size(&self) -> usize {
        self.inner.lock().len()
    }
    fn merge(&self, other: &PyGSet) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
    fn elements(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for v in self.inner.lock().iter() {
            list.append(v.clone())?;
        }
        Ok(list.unbind())
    }
}

#[pyclass(name = "ORSet", module = "atomr._native.ddata")]
pub struct PyORSet {
    inner: Mutex<OrSet<String>>,
}

#[pymethods]
impl PyORSet {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(OrSet::new()) }
    }
    fn add(&self, item: String) {
        self.inner.lock().add(item);
    }
    fn remove(&self, item: String) {
        self.inner.lock().remove(&item);
    }
    fn contains(&self, item: String) -> bool {
        self.inner.lock().contains(&item)
    }
    fn merge(&self, other: &PyORSet) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
    /// Snapshot of current set elements.
    fn elements(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for v in self.inner.lock().iter() {
            list.append(v.clone())?;
        }
        Ok(list.unbind())
    }
}

/// Pruning bookkeeping for CRDTs after a node leaves the cluster.
#[pyclass(name = "PruningState", module = "atomr._native.ddata")]
pub struct PyPruningState {
    inner: Mutex<PruningState>,
}

#[pymethods]
impl PyPruningState {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(PruningState::new()) }
    }

    fn initialize(&self, removed_node: String, owner: String) {
        self.inner.lock().initialize(removed_node, owner);
    }

    fn mark_performed(&self, removed_node: String, obsolete_at: u64) -> bool {
        self.inner.lock().mark_performed(&removed_node, obsolete_at)
    }

    fn is_pruned(&self, removed_node: String) -> bool {
        self.inner.lock().is_pruned(&removed_node)
    }

    fn owner(&self, removed_node: String) -> Option<String> {
        self.inner.lock().owner(&removed_node).map(|s| s.to_string())
    }

    fn gc(&self, current_round: u64) -> usize {
        self.inner.lock().gc(current_round)
    }

    fn merge(&self, other: &PyPruningState) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }

    /// Phase string for `removed_node`: `"initialized"`, `"performed"`, or
    /// `None` if the node isn't tracked.
    fn phase(&self, removed_node: String) -> Option<String> {
        let g = self.inner.lock();
        match g.markers.get(&removed_node)? {
            PruningPhase::Initialized { .. } => Some("initialized".into()),
            PruningPhase::Performed { .. } => Some("performed".into()),
        }
    }
}

// PruningPhase isn't strictly needed in Python (we expose strings) but
// having a re-export of the variant names is cheap.
#[pyfunction]
fn pruning_phases() -> Vec<String> {
    vec!["initialized".into(), "performed".into()]
}

/// Quorum-write tracker..
#[pyclass(name = "WriteAggregator", module = "atomr._native.ddata")]
pub struct PyWriteAggregator {
    inner: Mutex<WriteAggregator>,
}

#[pymethods]
impl PyWriteAggregator {
    #[new]
    fn new(target: usize) -> Self {
        Self { inner: Mutex::new(WriteAggregator::new(target)) }
    }
    fn ack(&self) {
        self.inner.lock().ack();
    }
    fn nack(&self) {
        self.inner.lock().nack();
    }
    fn is_satisfied(&self) -> bool {
        self.inner.lock().is_satisfied()
    }
    fn is_failed(&self, cluster_size: usize) -> bool {
        self.inner.lock().is_failed(cluster_size)
    }
    #[getter]
    fn received(&self) -> usize {
        self.inner.lock().received()
    }
    #[getter]
    fn target(&self) -> usize {
        self.inner.lock().target()
    }
}

/// Quorum-read tracker..
#[pyclass(name = "ReadAggregator", module = "atomr._native.ddata")]
pub struct PyReadAggregator {
    inner: Mutex<ReadAggregator>,
}

#[pymethods]
impl PyReadAggregator {
    #[new]
    fn new(target: usize) -> Self {
        Self { inner: Mutex::new(ReadAggregator::new(target)) }
    }
    fn reply(&self) {
        self.inner.lock().reply();
    }
    fn is_satisfied(&self) -> bool {
        self.inner.lock().is_satisfied()
    }
    #[getter]
    fn target(&self) -> usize {
        self.inner.lock().target()
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "ddata")?;
    sub.add_class::<PyGCounter>()?;
    sub.add_class::<PyPNCounter>()?;
    sub.add_class::<PyGSet>()?;
    sub.add_class::<PyORSet>()?;
    sub.add_class::<PyPruningState>()?;
    sub.add_class::<PyWriteAggregator>()?;
    sub.add_class::<PyReadAggregator>()?;
    sub.add_function(wrap_pyfunction!(pruning_phases, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
