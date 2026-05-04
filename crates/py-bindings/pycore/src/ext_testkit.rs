//! `testkit` submodule: TestKit, TestProbe, EventFilter.

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
    m.add_submodule(&sub)?;
    Ok(())
}
