//! `Context` Python shim. The Rust `Context<A>` is not thread-safe, so we
//! don't hand it to Python directly. Instead, each Python call receives a
//! lightweight `Context` object populated with the bits the user cares
//! about (self_ref, path, sender) plus a curated subset of system APIs —
//! the [`Scheduler`] in particular — exposed via [`SystemHandle`].
//!
//! For the first shipping slice we expose read-only accessors,
//! `schedule_once` / `schedule_periodically` / `schedule_with_fixed_delay`,
//! and [`PyCancelable`]. `stop_self`/`spawn_child`/`stash`/`unstash_all`/
//! `watch`/`unwatch`/`set_receive_timeout` remain tracked in PORTING_TODO.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use atomr_core::actor::scheduler::{Scheduler, SchedulerHandle};
use atomr_core::actor::SystemHandle;

use crate::actor_ref::PyActorRef;
use crate::runtime::runtime;

/// Python handle to a scheduled action. Wraps an atomr-core [`SchedulerHandle`].
/// `cancel()` is idempotent; `is_cancelled()` reflects either an explicit
/// cancel or a periodic that has been stopped externally.
#[pyclass(name = "Cancelable", module = "atomr._native")]
pub struct PyCancelable {
    pub(crate) handle: Arc<SchedulerHandle>,
}

impl PyCancelable {
    pub fn new(handle: SchedulerHandle) -> Self {
        Self { handle: Arc::new(handle) }
    }
}

#[pymethods]
impl PyCancelable {
    /// Cancel the scheduled action. Idempotent — subsequent calls are no-ops.
    /// For one-shot timers, prevents delivery if not yet fired. For
    /// periodic timers, stops further firings.
    fn cancel(&self) {
        self.handle.cancel();
    }

    /// True iff [`Self::cancel`] has been called or the timer has otherwise
    /// been marked cancelled by the scheduler.
    fn is_cancelled(&self) -> bool {
        self.handle.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("<Cancelable cancelled={}>", self.handle.is_cancelled())
    }
}

#[pyclass(name = "Context", module = "atomr._native")]
pub struct PyContext {
    pub(crate) self_ref: Py<PyActorRef>,
    pub(crate) path: String,
    /// Weak handle to the system, used to reach the scheduler. `None`
    /// means scheduling is not available from this context (e.g. in tests
    /// that build a `Context` manually); it is always populated when the
    /// `PyActor` bridge constructs the context.
    pub(crate) system: Option<SystemHandle>,
}

impl PyContext {
    /// Construct from the running `Context<PyActor>` — used by the
    /// `PyActor` bridge inside `handle`.
    pub fn from_handle(self_ref: Py<PyActorRef>, path: String, system: SystemHandle) -> Self {
        Self { self_ref, path, system: Some(system) }
    }
}

#[pymethods]
impl PyContext {
    #[getter]
    fn self_ref(&self, py: Python<'_>) -> Py<PyActorRef> {
        self.self_ref.clone_ref(py)
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Schedule `callback` to fire once after `delay` seconds.
    ///
    /// `callback` is a Python callable taking no arguments. Returns a
    /// [`PyCancelable`] handle; cancelling it before the timer fires
    /// prevents the callback from being invoked.
    fn schedule_once(&self, py: Python<'_>, delay: f64, callback: Py<PyAny>) -> PyResult<Py<PyCancelable>> {
        let scheduler = self.scheduler()?;
        let cb = callback;
        // The scheduler internally calls `tokio::spawn`, which requires a
        // current Tokio runtime. Python `handle` runs on the interpreter
        // worker thread (no runtime), so enter the shared runtime first.
        let _enter = runtime().enter();
        let handle = scheduler.schedule_once(
            Duration::from_secs_f64(delay),
            Box::pin(async move {
                Python::with_gil(|py| {
                    let bound = cb.bind(py);
                    let res = bound.call0();
                    if let Err(e) = res {
                        e.print(py);
                    }
                });
            }),
        );
        Py::new(py, PyCancelable::new(handle))
    }

    /// Schedule `callback` periodically. First firing is after
    /// `initial_delay` seconds, subsequent firings every `interval`
    /// seconds. Cancellation stops further firings.
    fn schedule_periodically(
        &self,
        py: Python<'_>,
        initial_delay: f64,
        interval: f64,
        callback: Py<PyAny>,
    ) -> PyResult<Py<PyCancelable>> {
        let scheduler = self.scheduler()?;
        let cb = Arc::new(callback);
        let cb_for_task = cb.clone();
        let task: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let cb = cb_for_task.clone();
            Python::with_gil(|py| {
                let bound = cb.bind(py);
                if let Err(e) = bound.call0() {
                    e.print(py);
                }
            });
        });
        let _enter = runtime().enter();
        let handle = scheduler.schedule_at_fixed_rate(
            Duration::from_secs_f64(initial_delay),
            Duration::from_secs_f64(interval),
            task,
        );
        Py::new(py, PyCancelable::new(handle))
    }

    /// Alias for [`Self::schedule_periodically`]. The scheduler currently
    /// uses fixed-rate semantics; if a future implementation distinguishes
    /// fixed-delay from fixed-rate we can switch the dispatch here without
    /// breaking callers.
    fn schedule_with_fixed_delay(
        &self,
        py: Python<'_>,
        initial_delay: f64,
        interval: f64,
        callback: Py<PyAny>,
    ) -> PyResult<Py<PyCancelable>> {
        self.schedule_periodically(py, initial_delay, interval, callback)
    }

    fn __repr__(&self) -> String {
        format!("<Context path={}>", self.path)
    }
}

impl PyContext {
    /// Resolve the scheduler from the system handle, raising a Python
    /// error if the system has been dropped or the context was built
    /// without a handle.
    fn scheduler(&self) -> PyResult<Arc<dyn Scheduler>> {
        let Some(handle) = &self.system else {
            return Err(PyErr::new::<crate::errors::AtomrError, _>(
                "Context built without a SystemHandle (test context?) — scheduling unavailable",
            ));
        };
        handle.scheduler().ok_or_else(|| {
            PyErr::new::<crate::errors::AtomrError, _>("ActorSystem terminated; scheduler unavailable")
        })
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()?;
    m.add_class::<PyCancelable>()?;
    Ok(())
}
