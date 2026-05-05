//! Python streams submodule. Exposes the `atomr-streams` DSL through a
//! small Python surface: `map_reduce` (Python-only convenience) plus
//! `run_collect`/`run_fold` entry-points that materialize a source
//! (defined by a Python iterable + a Python mapper closure) using the
//! native Tokio runtime.
//!
//! Streams operators are highly generic in Rust (they operate on
//! arbitrary `T: Send + 'static`). Exposing the full DSL with Python
//! callbacks would be possible but would force every type-erased value
//! through the GIL on each element, defeating the throughput-optimised
//! design of the underlying crate. The Python surface therefore offers
//! integer-typed wrappers — `via_keep_alive`, `via_initial_delay`,
//! `via_conflate`, `via_expand`, `merge_sorted`, `merge_prioritized`,
//! `via_split_after`, `via_prefix_and_tail`, `via_recover_with_retries`,
//! `via_select_error` — that demonstrate each operator's behaviour and
//! exercise the same Rust code paths.

use std::time::Duration;

use atomr_streams::{
    conflate, expand, initial_delay, keep_alive, merge_prioritized, merge_sorted, prefix_and_tail,
    recover_with_retries, select_error, split_after, ActorMaterializer, Flow, Sink, Source,
};
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

fn run_to_vec<F>(py: Python<'_>, build: F) -> Vec<i64>
where
    F: FnOnce() -> Source<i64> + Send,
{
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mat = ActorMaterializer::new();
            mat.run_collect(build()).await
        })
    })
}

/// `keep_alive(items, idle_secs, filler)` — emit `filler` after each
/// quiet interval of `idle_secs`. akka.net: `KeepAlive`.
#[pyfunction]
fn via_keep_alive(py: Python<'_>, items: Vec<i64>, idle_secs: f64, filler: i64) -> Vec<i64> {
    run_to_vec(py, move || keep_alive(Source::from_iter(items), Duration::from_secs_f64(idle_secs), move || filler))
}

/// `initial_delay(items, delay_secs)` — delay the first element.
/// akka.net: `InitialDelay`.
#[pyfunction]
fn via_initial_delay(py: Python<'_>, items: Vec<i64>, delay_secs: f64) -> Vec<i64> {
    run_to_vec(py, move || initial_delay(Source::from_iter(items), Duration::from_secs_f64(delay_secs)))
}

/// `conflate(items, fold)` — coalesce backed-up upstream elements.
/// akka.net: `Conflate`. The first element of each conflation window
/// becomes the seed; `fold(acc, next)` folds subsequent elements.
#[pyfunction]
fn via_conflate(py: Python<'_>, items: Vec<i64>, fold: Py<PyAny>) -> Vec<i64> {
    let fold_ref = fold;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = conflate(Source::from_iter(items), |x: i64| x, move |acc, v| {
                Python::with_gil(|gil| -> PyResult<i64> {
                    let r = fold_ref.call1(gil, (acc, v))?;
                    r.extract::<i64>(gil)
                })
                .unwrap_or(acc)
            });
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `expand(items, extrapolate)` — replace each element with an iterator
/// of values via `extrapolate(x) -> List[int]`. akka.net: `Expand`.
#[pyfunction]
fn via_expand(py: Python<'_>, items: Vec<i64>, extrapolate: Py<PyAny>) -> Vec<i64> {
    let cb = extrapolate;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = expand(Source::from_iter(items), move |x: &i64| {
                let xv = *x;
                let v: Vec<i64> = Python::with_gil(|gil| -> PyResult<Vec<i64>> {
                    let r = cb.call1(gil, (xv,))?;
                    r.extract::<Vec<i64>>(gil)
                })
                .unwrap_or_else(|_| vec![xv]);
                v.into_iter()
            });
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `merge_sorted(left, right)` — merge two sorted streams preserving
/// total order. akka.net: `MergeSorted`.
#[pyfunction]
fn merge_sorted_(py: Python<'_>, left: Vec<i64>, right: Vec<i64>) -> Vec<i64> {
    run_to_vec(py, move || merge_sorted(Source::from_iter(left), Source::from_iter(right)))
}

/// `merge_prioritized(left, left_weight, right, right_weight)` — merge
/// with weighted bias to one input. Weights must be ≥ 1.
/// akka.net: `MergePrioritized`.
#[pyfunction]
fn merge_prioritized_(
    py: Python<'_>,
    left: Vec<i64>,
    left_weight: u32,
    right: Vec<i64>,
    right_weight: u32,
) -> Vec<i64> {
    run_to_vec(py, move || {
        merge_prioritized(Source::from_iter(left), left_weight, Source::from_iter(right), right_weight)
    })
}

/// `split_after(items, predicate)` — split into substreams every time
/// `predicate(x)` is true; element causing the split lands in the
/// previous substream. Returns the count of substreams emitted.
/// akka.net: `SplitAfter`.
#[pyfunction]
fn via_split_after_count(py: Python<'_>, items: Vec<i64>, pred: Py<PyAny>) -> usize {
    let cb = pred;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = split_after(Source::from_iter(items), move |x: &i64| {
                Python::with_gil(|gil| {
                    cb.call1(gil, (*x,))
                        .and_then(|r| r.extract::<bool>(gil))
                        .unwrap_or(false)
                })
            });
            // Count emitted substreams
            ActorMaterializer::new().run_collect(s).await.len()
        })
    })
}

/// `prefix_and_tail(items, n)` — return the first `n` elements as a
/// list and the count of remaining tail elements.
/// akka.net: `PrefixAndTail`.
#[pyfunction]
fn via_prefix_and_tail(py: Python<'_>, items: Vec<i64>, n: usize) -> (Vec<i64>, usize) {
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = prefix_and_tail(Source::from_iter(items), n);
            let collected = ActorMaterializer::new().run_collect(s).await;
            // collected is Vec<(Vec<i64>, Source<i64>)>; in practice n>=1 yields one tuple.
            if let Some((prefix, tail)) = collected.into_iter().next() {
                let tail_count = ActorMaterializer::new().run_collect(tail).await.len();
                (prefix, tail_count)
            } else {
                (Vec::new(), 0)
            }
        })
    })
}

/// `recover_with_retries(items_with_errors, replacement, attempts)` —
/// each item is `(value, is_error)`. On error, replay `replacement` up
/// to `attempts` times. akka.net: `RecoverWithRetries`.
#[pyfunction]
fn via_recover_with_retries(
    py: Python<'_>,
    items_with_errors: Vec<(i64, bool)>,
    replacement: Vec<i64>,
    attempts: usize,
) -> Vec<i64> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mapped: Vec<Result<i64, ()>> =
                items_with_errors.into_iter().map(|(v, e)| if e { Err(()) } else { Ok(v) }).collect();
            let src = Source::from_iter(mapped);
            let s = recover_with_retries(src, attempts, move || Source::from_iter(replacement.clone()));
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `select_error(items, mapper)` — map error variants through a Python
/// callback. The Python callback receives the original error label and
/// returns a replacement label. akka.net: `SelectError`.
#[pyfunction]
fn via_select_error(py: Python<'_>, items_with_errors: Vec<(i64, Option<String>)>, mapper: Py<PyAny>) -> Vec<i64> {
    let cb = mapper;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mapped: Vec<Result<i64, String>> =
                items_with_errors.into_iter().map(|(v, e)| match e {
                    None => Ok(v),
                    Some(label) => Err(label),
                }).collect();
            let src = Source::from_iter(mapped);
            let mapped_src = select_error(src, move |label: String| -> String {
                Python::with_gil(|gil| {
                    cb.call1(gil, (label.clone(),))
                        .and_then(|r| r.extract::<String>(gil))
                        .unwrap_or(label)
                })
            });
            // Drop the (now mapped-error) Result<T, String>; keep only Ok values.
            let s: Source<i64> = mapped_src.filter_map(|r| r.ok());
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "streams")?;
    sub.add_function(wrap_pyfunction!(map_reduce, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_collect, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_fold, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_keep_alive, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_initial_delay, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_conflate, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_expand, &sub)?)?;
    sub.add_function(wrap_pyfunction!(merge_sorted_, &sub)?)?;
    sub.add_function(wrap_pyfunction!(merge_prioritized_, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_split_after_count, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_prefix_and_tail, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_recover_with_retries, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_select_error, &sub)?)?;
    let _ = pyo3_tokio::get_runtime;
    m.add_submodule(&sub)?;
    Ok(())
}
