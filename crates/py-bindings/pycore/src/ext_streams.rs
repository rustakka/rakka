//! Python streams submodule. Exposes the `atomr-streams` DSL through a
//! small Python surface: `map_reduce` (Python-only convenience) plus
//! `run_collect`/`run_fold` entry-points that materialize a source
//! (defined by a Python iterable + a Python mapper closure) using the
//! native Tokio runtime.

use atomr_streams::{ActorMaterializer, Flow, Sink, Source};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio as pyo3_tokio;

/// `map_reduce(iter, transform, reducer, zero)` — synchronous one-shot
/// pipeline. Runs entirely in Python; kept for backward compatibility
/// with the previous Python binding.
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

/// `run_collect(items, mapper)` — drives the native materializer. Items
/// are materialized once into a `Vec<i64>`; the Python mapper is called
/// synchronously per element (released GIL around the blocking future).
#[pyfunction]
fn run_collect(py: Python<'_>, items: Vec<i64>, mapper: Py<PyAny>) -> PyResult<Vec<i64>> {
    let mapper_ref = mapper.clone_ref(py);
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rt.block_on(async move {
            let source = Source::from_iter(items);
            let flow: Flow<i64, i64> = Flow::from_fn(move |x| {
                Python::with_gil(|py| {
                    let v = mapper_ref.call1(py, (x,))?;
                    v.extract::<i64>(py)
                })
                .unwrap_or(0)
            });
            let mat = ActorMaterializer::new();
            Ok::<_, PyErr>(mat.run_collect(source.via(flow)).await)
        })
    })
}

/// `run_fold(items, mapper, zero, reducer)` — drives the materializer and
/// accumulates an `i64` fold on top of the mapped elements.
#[pyfunction]
fn run_fold(
    py: Python<'_>,
    items: Vec<i64>,
    mapper: Py<PyAny>,
    zero: i64,
    reducer: Py<PyAny>,
) -> PyResult<i64> {
    let mapper_ref = mapper.clone_ref(py);
    let reducer_ref = reducer.clone_ref(py);
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rt.block_on(async move {
            let source = Source::from_iter(items).map(move |x| {
                Python::with_gil(|py| -> PyResult<i64> {
                    let v = mapper_ref.call1(py, (x,))?;
                    v.extract::<i64>(py)
                })
                .unwrap_or(0)
            });
            Ok::<_, PyErr>(
                Sink::fold(source, zero, move |acc, v| {
                    Python::with_gil(|py| -> PyResult<i64> {
                        let res = reducer_ref.call1(py, (acc, v))?;
                        res.extract::<i64>(py)
                    })
                    .unwrap_or(acc)
                })
                .await,
            )
        })
    })
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "streams")?;
    sub.add_function(wrap_pyfunction!(map_reduce, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_collect, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_fold, &sub)?)?;
    let _ = pyo3_tokio::get_runtime;
    m.add_submodule(&sub)?;
    Ok(())
}
