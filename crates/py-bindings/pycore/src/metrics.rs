//! Aggregate metrics exposed to Python callers — mostly a thin facade over
//! `InterpreterRegistry` snapshots.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::actor_system::registry;

#[pyfunction]
fn interpreter_metrics(py: Python<'_>) -> PyResult<Bound<'_, PyList>> {
    let list = PyList::empty_bound(py);
    for label in registry().labels() {
        if let Some(inst) = registry().get(&label) {
            let d = PyDict::new_bound(py);
            d.set_item("label", inst.label.clone())?;
            d.set_item("kind", inst.kind.as_str())?;
            let s = inst.snapshot_metrics();
            d.set_item("actors_hosted", s.actors_hosted)?;
            d.set_item("messages_handled", s.messages_handled)?;
            d.set_item("gil_hold_ns_total", s.gil_hold_ns_total)?;
            d.set_item("mailbox_depth_total", s.mailbox_depth_total)?;
            d.set_item("handler_panics", s.handler_panics)?;
            d.set_item("long_handlers", s.long_handlers)?;
            list.append(d)?;
        }
    }
    Ok(list)
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(interpreter_metrics, m)?)?;
    Ok(())
}
