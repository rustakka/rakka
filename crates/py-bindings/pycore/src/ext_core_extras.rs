//! Bindings for atomr-core types that don't have a natural home in the
//! existing top-level extension files: `DispatcherConfig`,
//! `BoundedStash`, `ControlAwareQueue`, `ResizerConfig`,
//! `DeadLetterFilter` / `DeadLetterReason`, plus a Python-driven
//! `FsmBuilder` over string state names and JSON-typed data.
//!
//! Hard-skip notes:
//!  * `Extensions` registry: keyed by Rust `TypeId`, which Python
//!    cannot supply. The registry is only useful from Rust extensions.
//!  * `ListenerRouter`: takes `RustRef<Listener>` typed actor refs;
//!    exposing it requires the typed-actor binding to be wired through
//!    the listener trait. Skipped.
//!  * `TcpManager::Connect`: pulled into atomr-remote which already has
//!    its own (unrelated) Python surface in pyremote.

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;

use atomr_core::actor::{BoundedStash, StashOverflow, StashResult};
use atomr_core::dispatch::dispatcher::DispatcherConfig;
use atomr_core::dispatch::mailbox::OverflowStrategy;
use atomr_core::dispatch::message_queues::{ControlAware, ControlAwareQueue};
use atomr_core::event::{DeadLetterFilter, DeadLetterReason};
use atomr_core::routing::ResizerConfig;

/// Throughput / deadline knobs for any dispatcher. akka.net:
/// `Dispatcher` config keys.
#[pyclass(name = "DispatcherConfig", module = "atomr._native.core")]
#[derive(Clone)]
pub struct PyDispatcherConfig {
    pub(crate) inner: DispatcherConfig,
}

#[pymethods]
impl PyDispatcherConfig {
    #[new]
    #[pyo3(signature = (throughput=10, throughput_deadline_secs=None))]
    fn new(throughput: u32, throughput_deadline_secs: Option<f64>) -> Self {
        Self {
            inner: DispatcherConfig {
                throughput,
                throughput_deadline: throughput_deadline_secs.map(Duration::from_secs_f64),
            },
        }
    }

    #[getter]
    fn throughput(&self) -> u32 {
        self.inner.throughput
    }

    #[getter]
    fn throughput_deadline_secs(&self) -> Option<f64> {
        self.inner.throughput_deadline.map(|d| d.as_secs_f64())
    }
}

/// Bounded mailbox overflow strategy. akka.net:
/// `BoundedMessageQueueSettings` policy.
#[pyclass(name = "OverflowStrategy", module = "atomr._native.core")]
#[derive(Clone, Copy)]
pub struct PyOverflowStrategy {
    pub(crate) inner: OverflowStrategy,
}

#[pymethods]
impl PyOverflowStrategy {
    #[new]
    fn new(name: String) -> PyResult<Self> {
        let inner = match name.as_str() {
            "drop_new" => OverflowStrategy::DropNew,
            "drop_head" => OverflowStrategy::DropHead,
            "drop_tail" => OverflowStrategy::DropTail,
            "fail" => OverflowStrategy::Fail,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown overflow strategy: {other:?}"
                )))
            }
        };
        Ok(Self { inner })
    }

    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            OverflowStrategy::DropNew => "drop_new",
            OverflowStrategy::DropHead => "drop_head",
            OverflowStrategy::DropTail => "drop_tail",
            OverflowStrategy::Fail => "fail",
        }
    }
}

/// Stash overflow policy for [`BoundedStash`].
#[pyclass(name = "StashOverflow", module = "atomr._native.core")]
#[derive(Clone, Copy)]
pub struct PyStashOverflow {
    pub(crate) inner: StashOverflow,
}

#[pymethods]
impl PyStashOverflow {
    #[new]
    fn new(name: String) -> PyResult<Self> {
        let inner = match name.as_str() {
            "drop_oldest" => StashOverflow::DropOldest,
            "drop_newest" => StashOverflow::DropNewest,
            "reject" => StashOverflow::Reject,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown stash overflow policy: {other:?}"
                )))
            }
        };
        Ok(Self { inner })
    }

    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            StashOverflow::DropOldest => "drop_oldest",
            StashOverflow::DropNewest => "drop_newest",
            StashOverflow::Reject => "reject",
            _ => "unknown",
        }
    }
}

/// Bounded stash buffer with a configurable overflow policy.
/// akka.net: `BoundedStash`.
#[pyclass(name = "BoundedStash", module = "atomr._native.core")]
pub struct PyBoundedStash {
    inner: Mutex<BoundedStash<Py<PyAny>>>,
}

#[pymethods]
impl PyBoundedStash {
    #[new]
    fn new(capacity: usize, policy: &PyStashOverflow) -> Self {
        Self { inner: Mutex::new(BoundedStash::new(capacity, policy.inner)) }
    }

    #[getter]
    fn capacity(&self) -> usize {
        self.inner.lock().capacity()
    }
    fn __len__(&self) -> usize {
        self.inner.lock().len()
    }
    fn is_full(&self) -> bool {
        self.inner.lock().is_full()
    }
    fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Push `msg` into the stash. Returns `("stashed", depth)` /
    /// `("dropped_oldest", displaced)` / `("dropped_newest", None)` /
    /// `("rejected", msg)`.
    fn stash(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let result = self.inner.lock().stash(msg);
        Ok(match result {
            StashResult::Stashed { depth } => ("stashed", depth as i64).into_py(py),
            StashResult::DroppedOldest(old) => ("dropped_oldest", old).into_py(py),
            StashResult::DroppedNewest => ("dropped_newest", py.None()).into_py(py),
            StashResult::Rejected(m) => ("rejected", m).into_py(py),
            _ => ("unknown", py.None()).into_py(py),
        })
    }

    /// Drain every stashed message.
    fn unstash_all(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyList>> {
        let v = self.inner.lock().unstash_all();
        let list = pyo3::types::PyList::empty_bound(py);
        for m in v {
            list.append(m)?;
        }
        Ok(list.unbind())
    }

    fn pop(&self, _py: Python<'_>) -> Option<Py<PyAny>> {
        self.inner.lock().pop()
    }
}

/// Control-aware mailbox. akka.net:
/// `UnboundedControlAwareMessageQueue`.
#[pyclass(name = "ControlAwareQueue", module = "atomr._native.core")]
pub struct PyControlAwareQueue {
    inner: Mutex<ControlAwareQueue<Py<PyAny>>>,
}

#[pymethods]
impl PyControlAwareQueue {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(ControlAwareQueue::new()) }
    }

    fn push_control(&self, msg: Py<PyAny>) {
        self.inner.lock().push(ControlAware::Control(msg));
    }

    fn push_user(&self, msg: Py<PyAny>) {
        self.inner.lock().push(ControlAware::User(msg));
    }

    fn pop(&self) -> Option<Py<PyAny>> {
        self.inner.lock().pop()
    }

    fn __len__(&self) -> usize {
        self.inner.lock().len()
    }

    fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// Resizer that decides how to grow / shrink an actor pool.
/// akka.net: `Resizer`.
#[pyclass(name = "ResizerConfig", module = "atomr._native.core")]
#[derive(Clone)]
pub struct PyResizerConfig {
    pub(crate) inner: ResizerConfig,
}

#[pymethods]
impl PyResizerConfig {
    #[new]
    #[pyo3(signature = (
        lower_bound=1,
        upper_bound=10,
        pressure_threshold=1.0,
        backoff_threshold=0.3,
        rampup_rate=0.2,
        backoff_rate=0.1,
        messages_per_resize=10,
        backoff_delay_secs=10.0,
    ))]
    fn new(
        lower_bound: usize,
        upper_bound: usize,
        pressure_threshold: f64,
        backoff_threshold: f64,
        rampup_rate: f64,
        backoff_rate: f64,
        messages_per_resize: u64,
        backoff_delay_secs: f64,
    ) -> Self {
        Self {
            inner: ResizerConfig {
                lower_bound,
                upper_bound,
                pressure_threshold,
                backoff_threshold,
                rampup_rate,
                backoff_rate,
                messages_per_resize,
                backoff_delay: Duration::from_secs_f64(backoff_delay_secs),
            },
        }
    }

    /// Compute the net change to apply given the current pool size and
    /// busy count. Returns the integer delta.
    fn compute_delta(&self, current_size: usize, busy: usize) -> i32 {
        self.inner.compute_delta(current_size, busy).delta
    }
}

/// Filter applied to dead letters before they reach the sink.
#[pyclass(name = "DeadLetterFilter", module = "atomr._native.core")]
#[derive(Clone, Copy)]
pub struct PyDeadLetterFilter {
    pub(crate) inner: DeadLetterFilter,
}

#[pymethods]
impl PyDeadLetterFilter {
    #[new]
    #[pyo3(signature = (accept_no_recipient=true, accept_dropped=true, accept_suppressed=false))]
    fn new(accept_no_recipient: bool, accept_dropped: bool, accept_suppressed: bool) -> Self {
        Self { inner: DeadLetterFilter { accept_no_recipient, accept_dropped, accept_suppressed } }
    }

    #[getter]
    fn accept_no_recipient(&self) -> bool {
        self.inner.accept_no_recipient
    }
    #[getter]
    fn accept_dropped(&self) -> bool {
        self.inner.accept_dropped
    }
    #[getter]
    fn accept_suppressed(&self) -> bool {
        self.inner.accept_suppressed
    }

    /// True if the filter accepts the given reason name (`no_recipient`,
    /// `dropped`, `suppressed`).
    fn accepts(&self, reason: String) -> PyResult<bool> {
        let r = match reason.as_str() {
            "no_recipient" => DeadLetterReason::NoRecipient,
            "dropped" => DeadLetterReason::Dropped,
            "suppressed" => DeadLetterReason::Suppressed,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown dead-letter reason: {other:?}"
                )))
            }
        };
        Ok(self.inner.accepts(r))
    }
}

/// Python-driven FSM builder. akka.net analog: `FSM<S, D>` /
/// `FsmBuilder` chained DSL.
///
/// State names and message tags are strings; data is an arbitrary
/// Python object. Each handler is called as `handler(state, data, msg)`
/// and returns `(new_state, new_data)` to transition or `None` to stay.
/// This binding mirrors the Rust builder semantics but executes purely
/// in Python — it does not embed into a Rust actor cell.
#[pyclass(name = "FsmBuilder", module = "atomr._native.core")]
pub struct PyFsmBuilder {
    initial_state: Mutex<Option<String>>,
    initial_data: Mutex<Option<Py<PyAny>>>,
    handlers: Mutex<HashMap<String, Py<PyAny>>>,
    fallback: Mutex<Option<Py<PyAny>>>,
    on_transition: Mutex<Option<Py<PyAny>>>,
    on_termination: Mutex<Option<Py<PyAny>>>,
}

#[pymethods]
impl PyFsmBuilder {
    #[new]
    fn new() -> Self {
        Self {
            initial_state: Mutex::new(None),
            initial_data: Mutex::new(None),
            handlers: Mutex::new(HashMap::new()),
            fallback: Mutex::new(None),
            on_transition: Mutex::new(None),
            on_termination: Mutex::new(None),
        }
    }

    fn start_with(slf: PyRef<'_, Self>, state: String, data: Py<PyAny>) -> PyRef<'_, Self> {
        *slf.initial_state.lock() = Some(state);
        *slf.initial_data.lock() = Some(data);
        slf
    }

    fn when_state(slf: PyRef<'_, Self>, state: String, handler: Py<PyAny>) -> PyRef<'_, Self> {
        slf.handlers.lock().insert(state, handler);
        slf
    }

    fn whenever(slf: PyRef<'_, Self>, handler: Py<PyAny>) -> PyRef<'_, Self> {
        *slf.fallback.lock() = Some(handler);
        slf
    }

    fn on_transition(slf: PyRef<'_, Self>, hook: Py<PyAny>) -> PyRef<'_, Self> {
        *slf.on_transition.lock() = Some(hook);
        slf
    }

    fn on_termination(slf: PyRef<'_, Self>, hook: Py<PyAny>) -> PyRef<'_, Self> {
        *slf.on_termination.lock() = Some(hook);
        slf
    }

    fn build(&self, py: Python<'_>) -> PyResult<Py<PyFsm>> {
        let state = self
            .initial_state
            .lock()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("FsmBuilder: start_with required"))?;
        let data = self
            .initial_data
            .lock()
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("FsmBuilder: start_with required"))?
            .clone_ref(py);
        let handlers = {
            let g = self.handlers.lock();
            g.iter().map(|(k, v)| (k.clone(), v.clone_ref(py))).collect()
        };
        let fallback = self.fallback.lock().as_ref().map(|f| f.clone_ref(py));
        let on_t = self.on_transition.lock().as_ref().map(|f| f.clone_ref(py));
        let on_term = self.on_termination.lock().as_ref().map(|f| f.clone_ref(py));
        Py::new(
            py,
            PyFsm {
                state: Mutex::new(state),
                data: Mutex::new(data),
                handlers,
                fallback,
                on_transition: on_t,
                on_termination: on_term,
            },
        )
    }
}

/// Built FSM. Drives one message at a time through `handle(msg)`.
#[pyclass(name = "Fsm", module = "atomr._native.core")]
pub struct PyFsm {
    state: Mutex<String>,
    data: Mutex<Py<PyAny>>,
    handlers: HashMap<String, Py<PyAny>>,
    fallback: Option<Py<PyAny>>,
    on_transition: Option<Py<PyAny>>,
    on_termination: Option<Py<PyAny>>,
}

#[pymethods]
impl PyFsm {
    #[getter]
    fn state(&self) -> String {
        self.state.lock().clone()
    }

    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyAny> {
        self.data.lock().clone_ref(py)
    }

    /// Process a message; if a handler returns `(new_state, new_data)`
    /// transition; otherwise stay.
    fn handle(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<()> {
        let cur_state = self.state.lock().clone();
        let cur_data = self.data.lock().clone_ref(py);
        let handler = self.handlers.get(&cur_state).map(|h| h.clone_ref(py));
        let result = if let Some(h) = handler {
            h.call1(py, (cur_state.clone(), cur_data.clone_ref(py), msg.clone_ref(py)))?
        } else if let Some(fb) = &self.fallback {
            fb.call1(py, (cur_state.clone(), cur_data.clone_ref(py), msg.clone_ref(py)))?
        } else {
            py.None()
        };
        if !result.is_none(py) {
            // Expect a tuple (new_state, new_data).
            let bound = result.bind(py);
            let new_state: String = bound.get_item(0)?.extract()?;
            let new_data: Py<PyAny> = bound.get_item(1)?.unbind();
            if new_state != cur_state {
                if let Some(hook) = &self.on_transition {
                    hook.call1(py, (cur_state.clone(), new_state.clone()))?;
                }
            }
            *self.state.lock() = new_state;
            *self.data.lock() = new_data;
        }
        Ok(())
    }

    /// Trigger termination — fires the `on_termination` hook if any.
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(hook) = &self.on_termination {
            let s = self.state.lock().clone();
            let d = self.data.lock().clone_ref(py);
            hook.call1(py, (s, d))?;
        }
        Ok(())
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "core")?;
    sub.add_class::<PyDispatcherConfig>()?;
    sub.add_class::<PyOverflowStrategy>()?;
    sub.add_class::<PyStashOverflow>()?;
    sub.add_class::<PyBoundedStash>()?;
    sub.add_class::<PyControlAwareQueue>()?;
    sub.add_class::<PyResizerConfig>()?;
    sub.add_class::<PyDeadLetterFilter>()?;
    sub.add_class::<PyFsmBuilder>()?;
    sub.add_class::<PyFsm>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
