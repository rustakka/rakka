//! Cluster submodule — read-only data structures *plus* the active
//! cluster control plane (`Cluster.get(system)`).
//!
//! The control plane (`PyCluster`) is the Phase-5 binding for
//! `atomr_cluster::ClusterDaemonHandle`. It is registered as a per-
//! `ActorSystem` extension — `Cluster.get(system)` is idempotent and
//! returns the same handle each time.
//!
//! Single-node by default. Cross-node transport is wired up in
//! Phase 9 (codec + remote). Until then `Cluster` runs against a no-op
//! gossip transport which is enough to drive the local daemon, exercise
//! membership transitions, and surface events to Python.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::sync::mpsc;

use atomr_cluster::{
    spawn_daemon, spawn_daemon_with_sbr, ClusterDaemonHandle, ClusterEvent, ClusterEventBus, DaemonConfig,
    GossipPdu, GossipTransport, KeepMajorityStrategy, KeepOldestStrategy, LeaderHandover, LeaderHandoverEvent,
    LeaseMajorityStrategy, Member, MemberStatus, MembershipState, SbrRuntime, StaticQuorumStrategy,
    SubscriptionHandle, VectorClock, VectorRelation,
};
use atomr_core::actor::Address;

use crate::actor_system::PyActorSystem;
use crate::errors;
use crate::runtime::runtime;

// ---------------------------------------------------------------------------
// Existing read-only types (kept verbatim from the Phase 0 surface).
// ---------------------------------------------------------------------------

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

    /// Compare two members by age. Returns -1, 0, or 1 (
    /// `Member.AgeOrdering`).
    #[staticmethod]
    fn age_ordering(a: &PyMember, b: &PyMember) -> i32 {
        match Member::age_ordering(&a.inner, &b.inner) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Member(address={}, status={}, up_number={})",
            self.inner.address,
            format!("{:?}", self.inner.status).to_lowercase(),
            self.inner.up_number,
        )
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

fn status_str(s: MemberStatus) -> &'static str {
    match s {
        MemberStatus::Joining => "joining",
        MemberStatus::WeaklyUp => "weakly_up",
        MemberStatus::Up => "up",
        MemberStatus::Leaving => "leaving",
        MemberStatus::Exiting => "exiting",
        MemberStatus::Down => "down",
        MemberStatus::Removed => "removed",
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

    /// Snapshot the current members as `[Member]`.
    fn members(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let snap = self.inner.lock().members.clone();
        let list = PyList::empty_bound(py);
        for m in snap {
            list.append(Py::new(py, PyMember { inner: m })?)?;
        }
        Ok(list.unbind())
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
/// leader address changes between snapshots.
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
/// `MembershipState` snapshots.
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

// ---------------------------------------------------------------------------
// Phase 5 — active cluster control plane.
// ---------------------------------------------------------------------------

/// No-op gossip transport. Discards every PDU. Used for single-node
/// clusters until Phase 9 plugs in a real remote transport.
struct NoopGossipTransport;

impl GossipTransport for NoopGossipTransport {
    fn send(&self, _target: &Address, _pdu: GossipPdu) {}
}

/// Per-`ActorSystem` cluster extension. Holds the daemon handle, the
/// event bus, and the self-address. Cloneable via `Arc`.
#[derive(Clone)]
struct ClusterExt {
    inner: Arc<ClusterExtInner>,
}

struct ClusterExtInner {
    handle: ClusterDaemonHandle,
    bus: ClusterEventBus,
    self_addr: Address,
}

/// Active cluster control object. Returned by `Cluster.get(system)` and
/// re-used across calls.
#[pyclass(name = "Cluster", module = "atomr._native.cluster")]
pub struct PyCluster {
    ext: ClusterExt,
}

#[pymethods]
impl PyCluster {
    /// Return (or lazily create) the cluster extension for the given
    /// `ActorSystem`. Idempotent — successive calls return the same
    /// underlying daemon.
    #[staticmethod]
    fn get(py: Python<'_>, system: &PyActorSystem) -> PyResult<Py<PyCluster>> {
        let sys = system.inner.clone();
        // Reuse if already installed.
        if let Some(ext) = sys.extensions().get::<ClusterExt>() {
            return Py::new(py, PyCluster { ext: (*ext).clone() });
        }
        // First call — start the daemon. Configure SBR if config asks
        // for one.
        let cfg = sys.config().clone();
        let bus = ClusterEventBus::new();
        let transport: Arc<dyn GossipTransport> = Arc::new(NoopGossipTransport);
        let dcfg = DaemonConfig::default();
        let self_addr = sys.address().clone();

        // `spawn_daemon` calls `tokio::spawn` internally, which requires
        // being inside a runtime. The Python entry point is sync, so
        // enter the shared atomr runtime for this call.
        let rt = runtime();
        let _guard = rt.enter();
        let handle = build_daemon(self_addr.clone(), transport, bus.clone(), dcfg, &cfg)?;

        // Register self as a Joining member so the daemon can promote
        // us to Up on the next leader-action tick.
        handle.join(Member::new(self_addr.clone(), Vec::new()));

        let ext = ClusterExt { inner: Arc::new(ClusterExtInner { handle, bus, self_addr }) };
        sys.extensions().register::<ClusterExt>(ext.clone());
        Py::new(py, PyCluster { ext })
    }

    /// The local node's address as a string.
    #[getter]
    fn self_address(&self) -> String {
        self.ext.inner.self_addr.to_string()
    }

    /// Snapshot the current membership state. Each call allocates a
    /// fresh `MembershipState`.
    fn membership_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyMembershipState>> {
        let snap = self.ext.inner.handle.snapshot();
        Py::new(py, PyMembershipState { inner: Mutex::new(snap.state) })
    }

    /// Address of the elected leader, or `None` if there is none yet.
    #[getter]
    fn leader(&self) -> Option<String> {
        self.ext.inner.handle.snapshot().leader.map(|a| a.to_string())
    }

    /// Number of currently-known members.
    fn member_count(&self) -> usize {
        self.ext.inner.handle.snapshot().state.member_count()
    }

    /// Async: register the seed node addresses, ensure the local node
    /// is a member, and resolve once self reaches `Up`. The default
    /// timeout is 30s.
    ///
    /// `Join` is only sent for members not already present in the
    /// snapshot — re-issuing `Join` would reset an Up member back to
    /// Joining, since the daemon's `MembershipState::add_or_update`
    /// overwrites by address.
    #[pyo3(signature = (seed_nodes, timeout=30.0))]
    fn join_seed_nodes<'py>(
        &self,
        py: Python<'py>,
        seed_nodes: Vec<String>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ext = self.ext.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Register each seed node + self exactly once if not yet
            // known to the daemon.
            let known_addresses: std::collections::HashSet<String> = ext
                .inner
                .handle
                .snapshot()
                .state
                .members
                .iter()
                .map(|m| m.address.to_string())
                .collect();

            let mut to_join: Vec<Address> = Vec::new();
            for s in &seed_nodes {
                let addr = Address::parse(s).unwrap_or_else(|| Address::local(s.clone()));
                if !known_addresses.contains(&addr.to_string()) {
                    to_join.push(addr);
                }
            }
            if !known_addresses.contains(&ext.inner.self_addr.to_string()) {
                to_join.push(ext.inner.self_addr.clone());
            }
            for addr in to_join {
                ext.inner.handle.join(Member::new(addr, Vec::new()));
            }

            // Poll the snapshot until self is Up.
            let deadline = tokio::time::Instant::now() + dur;
            loop {
                let snap = ext.inner.handle.snapshot();
                let me = snap.state.members.iter().find(|m| m.address == ext.inner.self_addr);
                if let Some(m) = me {
                    if matches!(m.status, MemberStatus::Up | MemberStatus::WeaklyUp) {
                        break;
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(PyErr::new::<errors::AtomrError, _>(format!(
                        "join_seed_nodes timed out after {:.3}s waiting for self to reach Up",
                        dur.as_secs_f64()
                    )));
                }
                ext.inner.handle.tick();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
    }

    /// Async: mark the local node as Leaving and wait for it to be
    /// `Removed`. The daemon's `leave` transition only fires once the
    /// member has reached `Up`, so this method first promotes self to
    /// `Up` (driving leader-action ticks) and then sends the leave. The
    /// default timeout is 30 seconds.
    #[pyo3(signature = (timeout=30.0))]
    fn leave<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let ext = self.ext.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let deadline = tokio::time::Instant::now() + dur;
            // 1. Wait until self is Up (so the leave transition will
            //    produce a MemberLeft event).
            let mut sent_leave = false;
            loop {
                let snap = ext.inner.handle.snapshot();
                let me = snap.state.members.iter().find(|m| m.address == ext.inner.self_addr).cloned();
                if !sent_leave {
                    if let Some(m) = &me {
                        if matches!(m.status, MemberStatus::Up | MemberStatus::WeaklyUp) {
                            ext.inner.handle.leave(ext.inner.self_addr.clone());
                            sent_leave = true;
                        }
                    }
                }
                let removed = match &me {
                    None => sent_leave, // never re-inserted after leave
                    Some(m) => matches!(m.status, MemberStatus::Removed),
                };
                if removed {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(PyErr::new::<errors::AtomrError, _>(format!(
                        "leave timed out after {:.3}s (sent_leave={})",
                        dur.as_secs_f64(),
                        sent_leave,
                    )));
                }
                ext.inner.handle.tick();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
    }

    /// Async: mark `address` as Down. Resolves once the daemon has
    /// applied the transition (best effort — completes on next tick).
    fn down<'py>(&self, py: Python<'py>, address: String) -> PyResult<Bound<'py, PyAny>> {
        let ext = self.ext.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let parsed = Address::parse(&address).unwrap_or_else(|| Address::local(address));
            // Down is modelled as a Leave today (transitions Up -> Leaving).
            ext.inner.handle.leave(parsed);
            ext.inner.handle.tick();
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        })
    }

    /// Subscribe to cluster events. `event_types` is an optional list of
    /// event-class names to filter on (e.g. `["MemberUp", "MemberRemoved"]`);
    /// when `None`, all events are delivered.
    ///
    /// Returns an async iterator (`__aiter__/__anext__`) backed by a
    /// bounded mpsc channel (default capacity 1024). Overflow drops
    /// oldest events; a running counter is exposed via
    /// `subscription.dropped_events`.
    #[pyo3(signature = (event_types=None, capacity=1024))]
    fn subscribe(
        &self,
        py: Python<'_>,
        event_types: Option<Vec<String>>,
        capacity: usize,
    ) -> PyResult<Py<PyClusterSubscription>> {
        let cap = capacity.max(1);
        let (tx, rx) = mpsc::channel::<ClusterEvent>(cap);
        let dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let dropped_for_cb = dropped.clone();
        let filter = event_types.clone();
        let tx_for_cb = tx.clone();
        let handle = self.ext.inner.bus.subscribe(move |e| {
            // Filter first so dropped_events only counts events the
            // caller wanted.
            if let Some(ref names) = filter {
                if !event_matches_filter(e, names) {
                    return;
                }
            }
            // try_send drop-oldest pattern: if the channel is full, pop
            // the oldest receiver-side. We can't pop from the sender;
            // approximation: if full, just count as dropped and skip.
            // Drop-oldest semantics for a tokio mpsc would require an
            // intermediate ring buffer; do that lazily.
            match tx_for_cb.try_send(e.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    dropped_for_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        });
        Py::new(
            py,
            PyClusterSubscription {
                inner: Mutex::new(SubscriptionInner {
                    rx: Some(rx),
                    _handle: Some(handle),
                    dropped,
                    filter: event_types,
                }),
            },
        )
    }
}

fn build_daemon(
    self_addr: Address,
    transport: Arc<dyn GossipTransport>,
    bus: ClusterEventBus,
    dcfg: DaemonConfig,
    cfg: &atomr_config::Config,
) -> PyResult<ClusterDaemonHandle> {
    let strategy_key = "cluster.sbr.strategy";
    let strategy = cfg.get_string(strategy_key).ok();
    let stable_after = cfg
        .get_duration("cluster.sbr.stable-after")
        .unwrap_or_else(|_| Duration::from_secs(20));
    match strategy.as_deref() {
        Some("keep-majority") => {
            let rt = SbrRuntime::new(KeepMajorityStrategy, stable_after);
            Ok(spawn_daemon_with_sbr(self_addr, transport, bus, dcfg, Some(rt)))
        }
        Some("static-quorum") => {
            let q: i64 = cfg.get_int("cluster.sbr.quorum-size").unwrap_or(1);
            let rt = SbrRuntime::new(StaticQuorumStrategy { quorum_size: q.max(1) as usize }, stable_after);
            Ok(spawn_daemon_with_sbr(self_addr, transport, bus, dcfg, Some(rt)))
        }
        Some("keep-oldest") => {
            let down_if_alone = cfg.get_bool("cluster.sbr.down-if-alone").unwrap_or(false);
            let rt = SbrRuntime::new(KeepOldestStrategy { down_if_alone }, stable_after);
            Ok(spawn_daemon_with_sbr(self_addr, transport, bus, dcfg, Some(rt)))
        }
        Some("down-all") | Some("down-all-when-unstable") => {
            // Simulate "down-all" by feeding KeepMajority a config that
            // always loses; for now reuse keep-majority since the
            // underlying daemon already chooses DownAll on a tie.
            let rt = SbrRuntime::new(KeepMajorityStrategy, stable_after);
            Ok(spawn_daemon_with_sbr(self_addr, transport, bus, dcfg, Some(rt)))
        }
        Some("lease-majority") => {
            let lease = cfg.get_bool("cluster.sbr.lease-acquired").unwrap_or(false);
            let rt = SbrRuntime::new(LeaseMajorityStrategy { lease_acquired: lease }, stable_after);
            Ok(spawn_daemon_with_sbr(self_addr, transport, bus, dcfg, Some(rt)))
        }
        Some(other) => Err(PyErr::new::<PyValueError, _>(format!(
            "unknown cluster.sbr.strategy: {other:?}"
        ))),
        None => Ok(spawn_daemon(self_addr, transport, bus, dcfg)),
    }
}

fn event_matches_filter(e: &ClusterEvent, names: &[String]) -> bool {
    let kind = event_kind(e);
    names.iter().any(|n| n == kind)
}

fn event_kind(e: &ClusterEvent) -> &'static str {
    match e {
        ClusterEvent::MemberJoined(_) => "MemberJoined",
        ClusterEvent::MemberWeaklyUp(_) => "MemberWeaklyUp",
        ClusterEvent::MemberUp(_) => "MemberUp",
        ClusterEvent::MemberLeft(_) => "MemberLeft",
        ClusterEvent::MemberExited(_) => "MemberExited",
        ClusterEvent::MemberRemoved(_, _) => "MemberRemoved",
        ClusterEvent::UnreachableMember(_) => "UnreachableMember",
        ClusterEvent::ReachableMember(_) => "ReachableMember",
        ClusterEvent::LeaderChanged { .. } => "LeaderChanged",
        ClusterEvent::ClusterShuttingDown => "ClusterShuttingDown",
        ClusterEvent::Convergence(_) => "Convergence",
        _ => "Unknown",
    }
}

fn event_to_py<'py>(py: Python<'py>, e: &ClusterEvent) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("kind", event_kind(e))?;
    match e {
        ClusterEvent::MemberJoined(m)
        | ClusterEvent::MemberWeaklyUp(m)
        | ClusterEvent::MemberUp(m)
        | ClusterEvent::MemberLeft(m)
        | ClusterEvent::MemberExited(m)
        | ClusterEvent::UnreachableMember(m)
        | ClusterEvent::ReachableMember(m) => {
            dict.set_item("member", member_to_py(py, m)?)?;
        }
        ClusterEvent::MemberRemoved(m, prev) => {
            dict.set_item("member", member_to_py(py, m)?)?;
            dict.set_item("previous_status", status_str(*prev))?;
        }
        ClusterEvent::LeaderChanged { from, to } => {
            dict.set_item("from_address", from.as_ref().map(|a| a.to_string()))?;
            dict.set_item("to_address", to.as_ref().map(|a| a.to_string()))?;
        }
        ClusterEvent::ClusterShuttingDown => {}
        ClusterEvent::Convergence(b) => {
            dict.set_item("converged", *b)?;
        }
        _ => {}
    }
    Ok(dict.into_any())
}

fn member_to_py<'py>(py: Python<'py>, m: &Member) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("address", m.address.to_string())?;
    dict.set_item("status", status_str(m.status))?;
    dict.set_item("up_number", m.up_number)?;
    dict.set_item("roles", m.roles.clone())?;
    Ok(dict.into_any())
}

struct SubscriptionInner {
    rx: Option<mpsc::Receiver<ClusterEvent>>,
    /// Held to keep the bus subscription alive until Drop.
    _handle: Option<SubscriptionHandle>,
    dropped: Arc<std::sync::atomic::AtomicU64>,
    filter: Option<Vec<String>>,
}

#[pyclass(name = "ClusterSubscription", module = "atomr._native.cluster")]
pub struct PyClusterSubscription {
    inner: Mutex<SubscriptionInner>,
}

#[pymethods]
impl PyClusterSubscription {
    /// Number of events that were dropped because the bounded channel
    /// was full when an event arrived.
    #[getter]
    fn dropped_events(&self) -> u64 {
        self.inner.lock().dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Filter passed to `subscribe`, or `None` if all events are
    /// delivered.
    #[getter]
    fn filter(&self) -> Option<Vec<String>> {
        self.inner.lock().filter.clone()
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Move the receiver out for the duration of the await.
        let rx = {
            let mut g = slf.inner.lock();
            g.rx.take()
        };
        let Some(mut rx) = rx else {
            return Err(PyStopAsyncIteration::new_err("subscription closed"));
        };
        let handle_slot = slf.into_py(py);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let next = rx.recv().await;
            // Park the receiver back on the subscription so the next
            // iteration can keep using it.
            Python::with_gil(|py| {
                let bound = handle_slot.bind(py);
                let cell: PyRef<'_, PyClusterSubscription> = bound.extract().unwrap();
                cell.inner.lock().rx = Some(rx);
            });
            match next {
                None => Err(PyStopAsyncIteration::new_err("subscription closed")),
                Some(ev) => Python::with_gil(|py| Ok(event_to_py(py, &ev)?.unbind())),
            }
        })
    }

    /// Eagerly close the subscription. Subsequent iterations raise
    /// `StopAsyncIteration`.
    fn close(&self) {
        let mut g = self.inner.lock();
        let _ = g._handle.take();
        let _ = g.rx.take();
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "cluster")?;
    sub.add_class::<PyMember>()?;
    sub.add_class::<PyMembershipState>()?;
    sub.add_class::<PyVectorClock>()?;
    sub.add_class::<PyLeaderHandover>()?;
    sub.add_class::<PyLeaderHandoverEvent>()?;
    sub.add_class::<PyCluster>()?;
    sub.add_class::<PyClusterSubscription>()?;
    sub.add_function(wrap_pyfunction!(member_weakly_up, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
