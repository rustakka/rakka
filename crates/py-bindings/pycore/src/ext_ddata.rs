//! Distributed Data CRDTs exposed to Python. Uses the Rust crate's
//! native types directly for deterministic merge semantics.

use parking_lot::Mutex;
use pyo3::prelude::*;

use rakka_distributed_data::{CrdtMerge, GCounter, GSet, OrSet, PNCounter};

#[pyclass(name = "GCounter", module = "rakka._native.ddata")]
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

#[pyclass(name = "PNCounter", module = "rakka._native.ddata")]
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

#[pyclass(name = "GSet", module = "rakka._native.ddata")]
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
}

#[pyclass(name = "ORSet", module = "rakka._native.ddata")]
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
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "ddata")?;
    sub.add_class::<PyGCounter>()?;
    sub.add_class::<PyPNCounter>()?;
    sub.add_class::<PyGSet>()?;
    sub.add_class::<PyORSet>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
