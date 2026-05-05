//! Telemetry submodule. dashboard / exporters
//! plumbing, but at the bus level.
//!
//! Exposes the topic catalog (`ALL_TOPICS`) and a topic-scoped
//! subscription helper (`subscribe_topic`) that yields a callable
//! `next()` returning the next event as a JSON dict.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::sync::mpsc;

use atomr_telemetry::bus::{TelemetryBus, TelemetryEvent};

use crate::runtime::runtime;

/// All known telemetry topic names. stable list shared with
/// the dashboard frontend.
#[pyfunction]
fn all_topics(py: Python<'_>) -> PyResult<Py<PyList>> {
    let list = PyList::empty_bound(py);
    for t in TelemetryEvent::ALL_TOPICS.iter() {
        list.append(*t)?;
    }
    Ok(list.unbind())
}

/// Lightweight Python-visible bus. Construct one and publish JSON
/// dicts; call `subscribe_topic` to receive forwarded events.
#[pyclass(name = "TelemetryBus", module = "atomr._native.telemetry")]
pub struct PyTelemetryBus {
    inner: TelemetryBus,
}

#[pymethods]
impl PyTelemetryBus {
    #[new]
    #[pyo3(signature = (capacity=1024))]
    fn new(capacity: usize) -> Self {
        Self { inner: TelemetryBus::new(capacity) }
    }

    /// Number of currently-attached subscribers.
    fn receiver_count(&self) -> usize {
        self.inner.receiver_count()
    }

    /// Subscribe to a single topic. Returns a `TopicSubscriber` whose
    /// `next(timeout)` yields the next event as a dict.
    fn subscribe_topic(&self, py: Python<'_>, topic: String) -> PyResult<Py<PyTopicSubscriber>> {
        // We keep a leaked &'static str because the bus signature wants
        // &'static; this binding is permanent for the bus lifetime.
        let leaked: &'static str = Box::leak(topic.into_boxed_str());
        let bus = self.inner.clone();
        let rt = runtime();
        // The forwarder task spawned inside subscribe_topic needs the
        // process-wide tokio runtime context.
        let rx = py.allow_threads(|| rt.block_on(async move { bus.subscribe_topic(leaked) }));
        Py::new(py, PyTopicSubscriber { rx: Arc::new(Mutex::new(Some(rx))) })
    }
}

#[pyclass(name = "TopicSubscriber", module = "atomr._native.telemetry")]
pub struct PyTopicSubscriber {
    rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<TelemetryEvent>>>>,
}

#[pymethods]
impl PyTopicSubscriber {
    /// Block up to `timeout_secs` for the next event, returning a dict
    /// or `None` on timeout.
    #[pyo3(signature = (timeout_secs=0.5))]
    fn next<'py>(&self, py: Python<'py>, timeout_secs: f64) -> PyResult<Option<Bound<'py, PyDict>>> {
        let rx = self.rx.clone();
        let rt = runtime();
        let ev = py.allow_threads(|| {
            // Temporarily take the receiver out of the mutex so we
            // never hold the parking_lot guard across an await point.
            let mut rx_inner = rx.lock().take()?;
            let result = rt.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs_f64(timeout_secs), rx_inner.recv())
                    .await
                    .ok()
                    .flatten()
            });
            *rx.lock() = Some(rx_inner);
            result
        });
        match ev {
            None => Ok(None),
            Some(e) => {
                let json = serde_json::to_value(&e).unwrap_or(serde_json::Value::Null);
                let dict = PyDict::new_bound(py);
                if let serde_json::Value::Object(map) = json {
                    for (k, v) in map {
                        dict.set_item(k, json_to_py(py, &v)?)?;
                    }
                }
                Ok(Some(dict))
            }
        }
    }
}

fn json_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        serde_json::Value::Null => py.None().into_bound(py),
        serde_json::Value::Bool(b) => b.into_py(py).into_bound(py),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py(py).into_bound(py)
            } else if let Some(f) = n.as_f64() {
                f.into_py(py).into_bound(py)
            } else {
                py.None().into_bound(py)
            }
        }
        serde_json::Value::String(s) => s.clone().into_py(py).into_bound(py),
        serde_json::Value::Array(items) => {
            let list = PyList::empty_bound(py);
            for v in items {
                list.append(json_to_py(py, v)?)?;
            }
            list.into_any()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            dict.into_any()
        }
    })
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "telemetry")?;
    sub.add_class::<PyTelemetryBus>()?;
    sub.add_class::<PyTopicSubscriber>()?;
    sub.add_function(wrap_pyfunction!(all_topics, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
