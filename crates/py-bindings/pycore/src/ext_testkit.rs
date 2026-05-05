//! `testkit` submodule: TestKit, TestProbe, EventFilter, multinode-oop
//! barrier helpers, and the new `expect_msg_eq` /
//! `expect_msg_all_of_in_order` / `within` matchers.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;
use tokio::sync::mpsc;

use crate::actor_ref::PyActorRef;
use crate::errors;
use crate::py_actor::{PyActor, PyMessage};
use crate::runtime::runtime;

use atomr_core::actor::{ActorRef as RustRef, ActorSystem as RustSystem, Props as RustProps};
use atomr_core::supervision::SupervisorStrategy;
use atomr_testkit::{MultiNodeOopController, MultiNodeOopNode};

/// A TestProbe is a lightweight actor that records every message received
/// and lets the caller assert on the stream.
#[pyclass(name = "TestProbe", module = "atomr._native.testkit")]
pub struct PyTestProbe {
    inbox: Arc<Mutex<Vec<Py<PyAny>>>>,
    notify_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>>,
    actor_ref: Py<PyActorRef>,
}

#[pymethods]
impl PyTestProbe {
    #[getter]
    fn ref_(&self, py: Python<'_>) -> Py<PyActorRef> {
        self.actor_ref.clone_ref(py)
    }

    fn messages(&self, py: Python<'_>) -> Py<PyAny> {
        let guard = self.inbox.lock();
        let list = pyo3::types::PyList::empty_bound(py);
        for m in guard.iter() {
            list.append(m.clone_ref(py)).ok();
        }
        list.unbind().into_any()
    }

    #[pyo3(signature = (timeout=1.0))]
    fn expect_message<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let inbox = self.inbox.clone();
        let notify_rx = self.notify_rx.clone();
        let dur = Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            {
                let mut guard = inbox.lock();
                if let Some(msg) = guard.pop() {
                    return Ok(msg);
                }
            }
            let recv_fut = async {
                let mut rx = notify_rx.lock().await;
                rx.recv().await
            };
            match tokio::time::timeout(dur, recv_fut).await {
                Ok(Some(())) => {
                    let mut guard = inbox.lock();
                    guard.pop().ok_or_else(|| PyErr::new::<errors::AtomrError, _>("probe spurious wakeup"))
                }
                _ => Err(PyErr::new::<errors::AtomrError, _>("probe timeout")),
            }
        })
    }

    /// Wait for one message and assert it equals `expected` using
    /// Python's `==`. /
    /// `expect_msg_eq`. `timeout` is in seconds.
    #[pyo3(signature = (expected, timeout=1.0))]
    fn expect_msg_eq<'py>(
        &self,
        py: Python<'py>,
        expected: Py<PyAny>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inbox = self.inbox.clone();
        let notify_rx = self.notify_rx.clone();
        let dur = Duration::from_secs_f64(timeout);
        let exp = expected;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let msg = pop_or_wait(&inbox, &notify_rx, dur).await?;
            Python::with_gil(|gil| {
                let lhs = msg.bind(gil);
                let rhs = exp.bind(gil);
                if lhs.eq(rhs)? {
                    Ok(msg.clone_ref(gil))
                } else {
                    Err(PyErr::new::<errors::AtomrError, _>("expected message did not match"))
                }
            })
        })
    }

    /// Wait for `len(expected)` messages and assert they appear in the
    /// exact order of `expected`. 
    /// `ExpectMsgAllOf` (sequential semantics).
    #[pyo3(signature = (expected, timeout=1.0))]
    fn expect_msg_all_of_in_order<'py>(
        &self,
        py: Python<'py>,
        expected: Vec<Py<PyAny>>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inbox = self.inbox.clone();
        let notify_rx = self.notify_rx.clone();
        let dur = Duration::from_secs_f64(timeout);
        let exp = expected;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let deadline = tokio::time::Instant::now() + dur;
            let mut received = Vec::with_capacity(exp.len());
            for _ in 0..exp.len() {
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .ok_or_else(|| PyErr::new::<errors::AtomrError, _>("probe timeout"))?;
                received.push(pop_or_wait(&inbox, &notify_rx, remaining).await?);
            }
            Python::with_gil(|gil| {
                for (got, want) in received.iter().zip(exp.iter()) {
                    let g = got.bind(gil);
                    let w = want.bind(gil);
                    if !g.eq(w)? {
                        return Err(PyErr::new::<errors::AtomrError, _>(
                            "expected ordered message stream did not match",
                        ));
                    }
                }
                Ok::<Py<PyAny>, PyErr>(gil.None())
            })
        })
    }
}

async fn pop_or_wait(
    inbox: &Arc<Mutex<Vec<Py<PyAny>>>>,
    notify_rx: &Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>>,
    dur: Duration,
) -> PyResult<Py<PyAny>> {
    {
        let mut guard = inbox.lock();
        if !guard.is_empty() {
            return Ok(guard.remove(0));
        }
    }
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| PyErr::new::<errors::AtomrError, _>("probe timeout"))?;
        let recv_fut = async {
            let mut rx = notify_rx.lock().await;
            rx.recv().await
        };
        match tokio::time::timeout(remaining, recv_fut).await {
            Ok(Some(())) => {
                let mut guard = inbox.lock();
                if !guard.is_empty() {
                    return Ok(guard.remove(0));
                }
            }
            _ => return Err(PyErr::new::<errors::AtomrError, _>("probe timeout")),
        }
    }
}

/// Run an async callable with a deadline. 
/// `Within(timeout, action)`. The callable is invoked with the timeout
/// in seconds so it can pass that to nested `expect_*` calls.
#[pyfunction]
#[pyo3(signature = (timeout, body))]
fn within<'py>(py: Python<'py>, timeout: f64, body: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let coro = Python::with_gil(|gil| -> PyResult<Py<PyAny>> {
            let r = body.call1(gil, (timeout,))?;
            Ok(r)
        })?;
        let fut = Python::with_gil(|gil| -> PyResult<_> {
            pyo3_async_runtimes::tokio::into_future(coro.bind(gil).clone())
        })?;
        match tokio::time::timeout(Duration::from_secs_f64(timeout), fut).await {
            Ok(r) => r,
            Err(_) => Err(PyErr::new::<errors::AtomrError, _>("within: deadline exceeded")),
        }
    })
}

/// Out-of-process barrier controller. 
/// `` controller side.
#[pyclass(name = "MultiNodeOopController", module = "atomr._native.testkit")]
pub struct PyMultiNodeOopController {
    inner: Mutex<Option<MultiNodeOopController>>,
    addr: SocketAddr,
}

#[pymethods]
impl PyMultiNodeOopController {
    #[new]
    fn new(py: Python<'_>, expected_nodes: usize) -> PyResult<Self> {
        let rt = runtime();
        let ctrl = py.allow_threads(|| rt.block_on(MultiNodeOopController::start(expected_nodes)));
        let ctrl = ctrl.map_err(|e| PyErr::new::<errors::AtomrError, _>(e.to_string()))?;
        let addr = ctrl.local_addr();
        Ok(Self { inner: Mutex::new(Some(ctrl)), addr })
    }

    /// Bound `host:port` of the controller's TCP listener.
    #[getter]
    fn local_addr(&self) -> String {
        self.addr.to_string()
    }

    /// Force `label` to time out after `timeout_secs`. Returns the
    /// number of nodes that arrived before the deadline.
    fn timeout_barrier(&self, py: Python<'_>, label: String, timeout_secs: f64) -> PyResult<usize> {
        let rt = runtime();
        let inner = self.inner.lock().as_ref().map(|c| c as *const MultiNodeOopController);
        match inner {
            None => Err(PyErr::new::<errors::AtomrError, _>("controller already shut down")),
            Some(ptr) => {
                // SAFETY: pointer is valid for the duration of this call —
                // we hold the controller through Mutex<Option<>> and never
                // drop while a mut borrow is active.
                let ctrl = unsafe { &*ptr };
                py.allow_threads(|| {
                    rt.block_on(async move {
                        ctrl.timeout_barrier(&label, Duration::from_secs_f64(timeout_secs))
                            .await
                            .map_err(|e| PyErr::new::<errors::AtomrError, _>(e.to_string()))
                    })
                })
            }
        }
    }

    fn shutdown(&self) {
        if let Some(c) = self.inner.lock().take() {
            c.shutdown();
        }
    }
}

/// Out-of-process barrier child node. 
/// `` node side.
#[pyclass(name = "MultiNodeOopNode", module = "atomr._native.testkit")]
pub struct PyMultiNodeOopNode {
    inner: Arc<MultiNodeOopNode>,
}

#[pymethods]
impl PyMultiNodeOopNode {
    /// Connect to a controller at `host:port`.
    #[staticmethod]
    fn connect(py: Python<'_>, controller_addr: String) -> PyResult<Self> {
        let rt = runtime();
        let parsed: SocketAddr = controller_addr
            .parse()
            .map_err(|e: std::net::AddrParseError| PyErr::new::<errors::AtomrError, _>(e.to_string()))?;
        let node = py.allow_threads(|| rt.block_on(MultiNodeOopNode::connect(parsed)));
        let node = node.map_err(|e| PyErr::new::<errors::AtomrError, _>(e.to_string()))?;
        Ok(Self { inner: Arc::new(node) })
    }

    /// Block until every peer has arrived on `label`.
    fn barrier(&self, py: Python<'_>, label: String) -> PyResult<()> {
        let rt = runtime();
        let inner = self.inner.clone();
        py.allow_threads(|| {
            rt.block_on(async move {
                inner.barrier(&label).await.map_err(|e| PyErr::new::<errors::AtomrError, _>(e.to_string()))
            })
        })
    }
}

/// A `TestKit` binds a fresh ActorSystem + helpers.
#[pyclass(name = "TestKit", module = "atomr._native.testkit")]
pub struct PyTestKit {
    pub(crate) system: RustSystem,
    pub(crate) next_probe: Mutex<u64>,
}

#[pymethods]
impl PyTestKit {
    #[new]
    #[pyo3(signature = (name="test-system".to_string()))]
    fn new(py: Python<'_>, name: String) -> PyResult<Self> {
        let rt = runtime();
        let system = py
            .allow_threads(|| rt.block_on(RustSystem::create(name, atomr_config::Config::empty())))
            .map_err(errors::map)?;
        Ok(Self { system, next_probe: Mutex::new(0) })
    }

    fn probe(&self, py: Python<'_>) -> PyResult<Py<PyTestProbe>> {
        let id = {
            let mut n = self.next_probe.lock();
            *n += 1;
            *n
        };
        let inbox = Arc::new(Mutex::new(Vec::<Py<PyAny>>::new()));
        let (ntx, nrx) = mpsc::unbounded_channel::<()>();
        let inbox_cl = inbox.clone();

        let props =
            RustProps::<ProbeActor>::create(move || ProbeActor { inbox: inbox_cl.clone(), tx: ntx.clone() });
        let name = format!("probe-{id}");
        let r: RustRef<PyMessage> = self.system.actor_of(props, &name).map_err(errors::map)?;
        let path = format!("akka://{}/user/{}", self.system.name(), name);
        let actor_ref = Py::new(py, PyActorRef::new(r, path))?;
        Py::new(py, PyTestProbe { inbox, notify_rx: Arc::new(tokio::sync::Mutex::new(nrx)), actor_ref })
    }

    fn shutdown(&self, py: Python<'_>) {
        let rt = runtime();
        let system = self.system.clone();
        py.allow_threads(|| rt.block_on(async move { system.terminate().await }));
    }
}

struct ProbeActor {
    inbox: Arc<Mutex<Vec<Py<PyAny>>>>,
    tx: mpsc::UnboundedSender<()>,
}

#[async_trait::async_trait]
impl atomr_core::actor::Actor for ProbeActor {
    type Msg = PyMessage;

    async fn handle(&mut self, _ctx: &mut atomr_core::actor::Context<Self>, msg: Self::Msg) {
        let payload = msg.payload;
        self.inbox.lock().push(payload);
        let _ = self.tx.send(());
    }

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        SupervisorStrategy::default()
    }
}

// avoid unused import warnings
#[allow(dead_code)]
fn _unused(_: &PyActor) {}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "testkit")?;
    sub.add_class::<PyTestKit>()?;
    sub.add_class::<PyTestProbe>()?;
    sub.add_class::<PyMultiNodeOopController>()?;
    sub.add_class::<PyMultiNodeOopNode>()?;
    sub.add_function(wrap_pyfunction!(within, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
