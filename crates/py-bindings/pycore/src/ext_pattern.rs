//! Phase 3 — resilience patterns: `CircuitBreaker`, `RetrySchedule`,
//! and `pipe_to`. Backoff lives in [`crate::ext_routing`] because it is
//! exposed as a `Props` factory.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use atomr_core::pattern::{CircuitBreaker, CircuitBreakerError, CircuitBreakerState, RetrySchedule};

use crate::actor_ref::PyActorRef;
use crate::errors;
use crate::py_actor::PyMessage;

// ============================================================================
// CircuitBreaker
// ============================================================================

/// Generic async circuit breaker. Wraps coroutines via `call_async`.
///
/// ```python
/// breaker = CircuitBreaker(max_failures=3, call_timeout=0.5, reset_timeout=2.0)
/// try:
///     result = await breaker.call_async(my_async_op())
/// except CircuitBreakerOpen:
///     ...
/// ```
#[pyclass(name = "CircuitBreaker", module = "atomr._native.pattern")]
pub struct PyCircuitBreaker {
    inner: Arc<CircuitBreaker>,
}

#[pymethods]
impl PyCircuitBreaker {
    #[new]
    #[pyo3(signature = (max_failures=5, call_timeout=1.0, reset_timeout=10.0))]
    fn new(max_failures: u32, call_timeout: f64, reset_timeout: f64) -> Self {
        Self {
            inner: CircuitBreaker::new(
                max_failures,
                Duration::from_secs_f64(call_timeout),
                Duration::from_secs_f64(reset_timeout),
            ),
        }
    }

    /// Current state — one of `"closed"`, `"open"`, `"half_open"`.
    #[getter]
    fn state(&self) -> &'static str {
        match self.inner.state() {
            CircuitBreakerState::Closed => "closed",
            CircuitBreakerState::Open => "open",
            CircuitBreakerState::HalfOpen => "half_open",
            _ => "unknown",
        }
    }

    /// Run the supplied coroutine through the breaker. Returns its
    /// result on success, raises `CircuitBreakerOpen` when the breaker
    /// is open, `AskError` on timeout, or re-raises the inner
    /// exception on a Python-level failure.
    #[pyo3(signature = (coro))]
    fn call_async<'py>(&self, py: Python<'py>, coro: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let breaker = self.inner.clone();
        let coro = coro;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let fut = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(coro.bind(py).clone())
            })?;
            let res: Result<Py<PyAny>, CircuitBreakerError<PyErr>> =
                breaker.call(move || fut).await;
            match res {
                Ok(v) => Ok(v),
                Err(CircuitBreakerError::Open) => {
                    Err(PyErr::new::<CircuitBreakerOpen, _>("circuit breaker is open"))
                }
                Err(CircuitBreakerError::Timeout) => {
                    Err(PyErr::new::<errors::AskError, _>("circuit breaker call timed out"))
                }
                Err(CircuitBreakerError::Inner(e)) => Err(e),
                Err(_) => Err(PyErr::new::<errors::AtomrError, _>("circuit breaker error")),
            }
        })
    }
}

pyo3::create_exception!(
    atomr,
    CircuitBreakerOpen,
    errors::AtomrError,
    "Circuit breaker rejected the call because it is open."
);

// ============================================================================
// RetrySchedule
// ============================================================================

/// Schedule of delays for `retry`. Use `RetrySchedule.fixed(secs)` or
/// `RetrySchedule.exponential(min_secs, max_secs)`.
#[pyclass(name = "RetrySchedule", module = "atomr._native.pattern")]
#[derive(Clone, Copy)]
pub struct PyRetrySchedule {
    pub(crate) inner: RetrySchedule,
}

#[pymethods]
impl PyRetrySchedule {
    /// Fixed delay between every attempt.
    #[staticmethod]
    fn fixed(seconds: f64) -> Self {
        Self { inner: RetrySchedule::fixed(Duration::from_secs_f64(seconds)) }
    }

    /// Exponential backoff: `min`, `min*2`, `min*4`, … capped at `max`.
    #[staticmethod]
    fn exponential(min_seconds: f64, max_seconds: f64) -> Self {
        Self {
            inner: RetrySchedule::exponential(
                Duration::from_secs_f64(min_seconds),
                Duration::from_secs_f64(max_seconds),
            ),
        }
    }

    /// Delay before the `attempt`th retry (0-indexed).
    fn delay_for(&self, attempt: u32) -> f64 {
        self.inner.delay_for(attempt).as_secs_f64()
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RetrySchedule::Fixed(d) => format!("RetrySchedule.fixed({:.6})", d.as_secs_f64()),
            RetrySchedule::Exponential { min, max } => format!(
                "RetrySchedule.exponential(min={:.6}, max={:.6})",
                min.as_secs_f64(),
                max.as_secs_f64()
            ),
            _ => "RetrySchedule(?)".to_string(),
        }
    }
}

/// `await retry(async_fn, max_attempts, schedule)` — runs `async_fn()`
/// repeatedly until it succeeds (returns a non-exception value) or the
/// budget is exhausted. `async_fn` must be a zero-argument callable that
/// returns a fresh awaitable per call.
#[pyfunction]
#[pyo3(signature = (async_fn, max_attempts, schedule))]
fn retry<'py>(
    py: Python<'py>,
    async_fn: Py<PyAny>,
    max_attempts: u32,
    schedule: PyRetrySchedule,
) -> PyResult<Bound<'py, PyAny>> {
    if max_attempts == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("max_attempts must be ≥ 1"));
    }
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut last_err: Option<PyErr> = None;
        for attempt in 0..max_attempts {
            let coro_res = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let r = async_fn.call0(py)?;
                Ok(r)
            });
            let coro = match coro_res {
                Ok(c) => c,
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < max_attempts {
                        tokio::time::sleep(schedule.inner.delay_for(attempt)).await;
                    }
                    continue;
                }
            };
            let fut_res = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(coro.bind(py).clone())
            });
            let fut = match fut_res {
                Ok(f) => f,
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < max_attempts {
                        tokio::time::sleep(schedule.inner.delay_for(attempt)).await;
                    }
                    continue;
                }
            };
            match fut.await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < max_attempts {
                        tokio::time::sleep(schedule.inner.delay_for(attempt)).await;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| PyErr::new::<errors::AtomrError, _>("retry exhausted")))
    })
}

// ============================================================================
// pipe_to
// ============================================================================

/// Schedule the awaitable; on completion, deliver its value to
/// `target` via `tell`. Returns `None` immediately. Errors raised by
/// the awaitable are *not* delivered — they are logged as a tracing
/// warning. For error-aware piping, wrap in a try/except on the
/// caller side.
#[pyfunction]
#[pyo3(signature = (awaitable, target))]
fn pipe_to<'py>(
    py: Python<'py>,
    awaitable: Py<PyAny>,
    target: Py<PyActorRef>,
) -> PyResult<Bound<'py, PyAny>> {
    let target_ref = target.borrow(py).inner.clone();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let fut = Python::with_gil(|py| {
            pyo3_async_runtimes::tokio::into_future(awaitable.bind(py).clone())
        })?;
        match fut.await {
            Ok(v) => {
                target_ref.tell(PyMessage::new(v));
                Ok(Python::with_gil(|py| py.None()))
            }
            Err(e) => {
                tracing::warn!("pipe_to: source future raised: {e}");
                Err(e)
            }
        }
    })
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "pattern")?;
    sub.add_class::<PyCircuitBreaker>()?;
    sub.add_class::<PyRetrySchedule>()?;
    sub.add_function(wrap_pyfunction!(retry, &sub)?)?;
    sub.add_function(wrap_pyfunction!(pipe_to, &sub)?)?;
    sub.add("CircuitBreakerOpen", py.get_type_bound::<CircuitBreakerOpen>())?;
    m.add_submodule(&sub)?;
    Ok(())
}
