//! Cluster submodule — a thin view over core cluster data structures.

use parking_lot::Mutex;
use pyo3::prelude::*;

use atomr_cluster::{
    LeaderHandover, LeaderHandoverEvent, Member, MemberStatus, MembershipState, VectorClock, VectorRelation,
};
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

    #[getter]
    fn up_number(&self) -> i32 {
        self.inner.up_number
    }

    #[getter]
    fn roles(&self) -> Vec<String> {
        self.inner.roles.clone()
    }

    fn with_status(&self, status: String) -> Self {
        let s = status_from_str(&status);
        Self { inner: self.inner.copy_with_status(s) }
    }

    /// Compare two members by age. Returns -1, 0, or 1 (akka.net:
    /// `Member.AgeOrdering`).
    #[staticmethod]
    fn age_ordering(a: &PyMember, b: &PyMember) -> i32 {
        match Member::age_ordering(&a.inner, &b.inner) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }
}

fn status_from_str(s: &str) -> MemberStatus {
    match s {
        "joining" => MemberStatus::Joining,
        "weakly_up" | "weaklyup" => MemberStatus::WeaklyUp,
        "up" => MemberStatus::Up,
        "leaving" => MemberStatus::Leaving,
        "exiting" => MemberStatus::Exiting,
        "down" => MemberStatus::Down,
        "removed" => MemberStatus::Removed,
        _ => MemberStatus::Joining,
    }
}

/// Convenience function exposing the `MemberStatus::WeaklyUp` variant
/// for callers that want to construct a `Member` in the WeaklyUp state.
#[pyfunction]
fn member_weakly_up(address: String, roles: Vec<String>) -> PyMember {
    PyMember { inner: Member::new(Address::local(address), roles).copy_with_status(MemberStatus::WeaklyUp) }
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

/// Event emitted by a `LeaderHandover` watcher when the elected
/// leader address changes between snapshots. akka.net analog:
/// `Cluster.LeaderChanged`.
#[pyclass(name = "LeaderHandoverEvent", module = "atomr._native.cluster")]
#[derive(Clone)]
pub struct PyLeaderHandoverEvent {
    pub(crate) inner: LeaderHandoverEvent,
}

#[pymethods]
impl PyLeaderHandoverEvent {
    #[getter]
    fn from_(&self) -> Option<String> {
        self.inner.from.as_ref().map(|a| a.to_string())
    }

    #[getter]
    fn to(&self) -> Option<String> {
        self.inner.to.as_ref().map(|a| a.to_string())
    }

    fn __repr__(&self) -> String {
        format!(
            "LeaderHandoverEvent(from={:?}, to={:?})",
            self.inner.from.as_ref().map(|a| a.to_string()),
            self.inner.to.as_ref().map(|a| a.to_string()),
        )
    }
}

/// Watcher that detects leader transitions across successive
/// `MembershipState` snapshots. akka.net analog:
/// `Akka.Cluster.LeaderChanged` event with stateful detection.
#[pyclass(name = "LeaderHandover", module = "atomr._native.cluster")]
pub struct PyLeaderHandover {
    inner: Mutex<LeaderHandover>,
}

#[pymethods]
impl PyLeaderHandover {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(LeaderHandover::new()) }
    }

    /// Observe a new membership snapshot. Returns the handover event if
    /// the leader changed, otherwise `None`.
    fn observe(&self, state: &PyMembershipState) -> Option<PyLeaderHandoverEvent> {
        let g = state.inner.lock();
        self.inner.lock().observe(&g).map(|e| PyLeaderHandoverEvent { inner: e })
    }

    /// The leader observed at the last call to `observe`.
    #[getter]
    fn current(&self) -> Option<String> {
        self.inner.lock().current().map(|a| a.to_string())
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster")?;
    sub.add_class::<PyMember>()?;
    sub.add_class::<PyMembershipState>()?;
    sub.add_class::<PyVectorClock>()?;
    sub.add_class::<PyLeaderHandover>()?;
    sub.add_class::<PyLeaderHandoverEvent>()?;
    sub.add_function(wrap_pyfunction!(member_weakly_up, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
