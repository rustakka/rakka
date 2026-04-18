//! Streams submodule (Phase P8 slice) — minimal Source/Sink using Python callables.

use pyo3::prelude::*;
use pyo3::types::PyList;

/// Collect-fold helper: feeds an iterable of items through a transform closure.
/// Runs entirely in Python; the real streams material needs the Rust
/// `rustakka-streams` materializer wired through pyo3-async-runtimes which
/// is tracked in PORTING_TODO under Phase P8.
#[pyfunction]
fn map_reduce(
    py: Python<'_>,
    source: Py<PyAny>,
    transform: Py<PyAny>,
    reducer: Py<PyAny>,
    zero: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let iter = source.bind(py).iter()?;
    let mut acc: Py<PyAny> = zero;
    for item in iter {
        let item = item?;
        let mapped = transform.call1(py, (item,))?;
        acc = reducer.call1(py, (acc, mapped))?;
    }
    Ok(acc)
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "streams")?;
    sub.add_function(wrap_pyfunction!(map_reduce, &sub)?)?;
    let _ = PyList::empty_bound(py);
    m.add_submodule(&sub)?;
    Ok(())
}
