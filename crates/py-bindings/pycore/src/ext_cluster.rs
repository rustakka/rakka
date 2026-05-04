//! Cluster submodule — a thin view over core cluster data structures.

use parking_lot::Mutex;
use pyo3::prelude::*;

use atomr_cluster::{Member, MemberStatus, MembershipState, VectorClock, VectorRelation};
use atomr_core::actor::Address;

#[pyclass(name = "Member", module = "atomr._native.cluster")]
#[derive(Clone)]
pub struct PyMember {
    pub(crate) inner: Member,
}

#[pymethods]
impl PyMember {
    #[new]
    #[pyo3(signature = (address, roles=Vec::new()))]
    fn new(address: String, roles: Vec<String>) -> Self {
        Self { inner: Member::new(Address::local(address), roles) }
    }

    #[getter]
    fn address(&self) -> String {
        self.inner.address.to_string()
    }

    #[getter]
    fn status(&self) -> String {
        format!("{:?}", self.inner.status).to_lowercase()
    }

    fn with_status(&self, status: String) -> Self {
        let s = match status.as_str() {
            "joining" => MemberStatus::Joining,
            "weakly_up" => MemberStatus::WeaklyUp,
            "up" => MemberStatus::Up,
            "leaving" => MemberStatus::Leaving,
            "exiting" => MemberStatus::Exiting,
            "down" => MemberStatus::Down,
            "removed" => MemberStatus::Removed,
            _ => MemberStatus::Joining,
        };
        Self { inner: self.inner.copy_with_status(s) }
    }
}

#[pyclass(name = "MembershipState", module = "atomr._native.cluster")]
pub struct PyMembershipState {
    pub(crate) inner: Mutex<MembershipState>,
}

#[pymethods]
impl PyMembershipState {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(MembershipState::new()) }
    }

    fn add_or_update(&self, m: Py<PyMember>, py: Python<'_>) {
        self.inner.lock().add_or_update(m.borrow(py).inner.clone());
    }

    fn member_count(&self) -> usize {
        self.inner.lock().member_count()
    }
}

#[pyclass(name = "VectorClock", module = "atomr._native.cluster")]
pub struct PyVectorClock {
    inner: Mutex<VectorClock>,
}

#[pymethods]
impl PyVectorClock {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(VectorClock::new()) }
    }
    fn tick(&self, node: String) {
        self.inner.lock().tick(&node);
    }
    fn compare(&self, other: &PyVectorClock) -> String {
        let me = self.inner.lock().clone();
        let them = other.inner.lock().clone();
        match me.compare(&them) {
            VectorRelation::Before => "before",
            VectorRelation::After => "after",
            VectorRelation::Same => "same",
            VectorRelation::Concurrent => "concurrent",
            _ => "unknown",
        }
        .to_string()
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster")?;
    sub.add_class::<PyMember>()?;
    sub.add_class::<PyMembershipState>()?;
    sub.add_class::<PyVectorClock>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
