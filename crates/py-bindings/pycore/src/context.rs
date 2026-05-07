//! `Context` Python shim. The Rust `Context<A>` is `!Send` and lives on
//! the actor's Tokio dispatcher task, so we cannot hand a `&mut Context`
//! to the interpreter worker. Instead, on every message dispatch we:
//!
//! 1. Snapshot value-typed fields (`self_ref` clone, `path.to_string()`,
//!    `sender` clone) into a fresh `PyContext` `#[pyclass]`.
//! 2. Hand the `PyContext` an `mpsc::UnboundedSender<CtxOp>` whose
//!    receiver lives in `PyActor::handle`.
//! 3. The Python coroutine pushes `CtxOp` values for spawn / watch /
//!    stash / become / etc.
//! 4. After the coroutine completes we drain the queue against the live
//!    `&mut Context<PyActor>` — preserving Akka semantics where context
//!    mutations take effect at end-of-message.
//!
//! For `ctx.spawn`, the Python-side blocks on a `oneshot` reply; the
//! Rust dispatcher must service `CtxOp::Spawn` *during* the coroutine's
//! await window. `PyActor::handle` does this with `tokio::select!`.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::actor_ref::PyActorRef;
use crate::py_actor::PyMessage;

/// Operations issued by a Python handler that need to be applied to the
/// live Rust `Context<PyActor>` after the handler returns (or, for
/// `Spawn`, eagerly during the await window).
pub enum CtxOp {
    /// Spawn a child PyActor under the current context. The reply
    /// channel ships back the resulting `PyActorRef` (or an error
    /// message on collision / system shutdown).
    Spawn {
        factory: Py<PyAny>,
        name: String,
        interpreter_role: String,
        dispatcher: String,
        reply: oneshot::Sender<Result<PyActorRef, String>>,
    },
    /// Stop a named child of this actor.
    StopChild(String),
    /// Stash the given Python message for a later `unstash_all`.
    Stash(Py<PyAny>),
    /// Drain stash and re-tell every message to self.
    UnstashAll,
    /// Begin self-stop.
    StopSelf,
    /// Set or clear the receive-idle timeout.
    SetReceiveTimeout(Option<Duration>),
    /// Schedule a one-shot timer that delivers `msg` to `target` (or
    /// self if `target` is `None`) after `delay`.
    ScheduleOnce {
        delay: Duration,
        msg: Py<PyAny>,
        target: Option<Arc<atomr_core::actor::ActorRef<PyMessage>>>,
    },
    /// Schedule a periodic timer.
    SchedulePeriodic {
        initial: Duration,
        interval: Duration,
        msg: Py<PyAny>,
        target: Option<Arc<atomr_core::actor::ActorRef<PyMessage>>>,
    },
    /// Replace the active handler with a new callable (Akka `become`).
    Become(Py<PyAny>),
    /// Restore the default handler (`instance.handle`).
    Unbecome,
    /// Begin watching `target`. Phase 2 — when `target` terminates,
    /// `PyActor::on_terminated` translates the framework `Terminated`
    /// system message into a Python-visible `Terminated(path)` user
    /// message tell-ed to self.
    Watch(Arc<atomr_core::actor::ActorRef<PyMessage>>),
    /// Stop watching `target`.
    Unwatch(Arc<atomr_core::actor::ActorRef<PyMessage>>),
}

/// Python-facing `Context`. One instance per dispatched message.
#[pyclass(name = "Context", module = "atomr._native")]
pub struct PyContext {
    pub(crate) self_ref: Py<PyActorRef>,
    pub(crate) path: String,
    pub(crate) sender: Option<Py<PyActorRef>>,
    /// Default child interpreter role inherited from the parent.
    pub(crate) interpreter_role: String,
    pub(crate) dispatcher: String,
    pub(crate) ops: Mutex<Option<mpsc::UnboundedSender<CtxOp>>>,
}

impl PyContext {
    pub fn new(
        self_ref: Py<PyActorRef>,
        path: String,
        sender: Option<Py<PyActorRef>>,
        interpreter_role: String,
        dispatcher: String,
        ops: mpsc::UnboundedSender<CtxOp>,
    ) -> Self {
        Self { self_ref, path, sender, interpreter_role, dispatcher, ops: Mutex::new(Some(ops)) }
    }

    /// After the handler returns, invalidate the op channel so any
    /// stale references held by user code can no longer push ops.
    pub fn invalidate(&self) {
        let mut g = self.ops.lock().unwrap();
        *g = None;
    }

    fn send_op(&self, op: CtxOp) -> PyResult<()> {
        let g = self.ops.lock().unwrap();
        match g.as_ref() {
            Some(tx) => tx.send(op).map_err(|_| PyRuntimeError::new_err("actor context channel closed")),
            None => Err(PyRuntimeError::new_err("Context used outside of its handler scope")),
        }
    }
}

#[pymethods]
impl PyContext {
    /// `ActorRef` to self.
    #[getter]
    fn self_ref(&self, py: Python<'_>) -> Py<PyActorRef> {
        self.self_ref.clone_ref(py)
    }

    /// Full path string (e.g. `"akka://S/user/foo"`).
    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Sender of the message currently being processed, if known.
    #[getter]
    fn sender(&self, py: Python<'_>) -> Option<Py<PyActorRef>> {
        self.sender.as_ref().map(|p| p.clone_ref(py))
    }

    /// Spawn a child actor under this context.
    ///
    /// Returns the new child's `ActorRef`. Children inherit this
    /// actor's interpreter role unless `props` overrides it.
    fn spawn<'py>(
        &self,
        py: Python<'py>,
        props: Py<crate::props::PyProps>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Pull factory + dispatcher + role out of the Props now so we
        // don't need the GIL on the dispatcher task.
        let pp_ref = props.borrow(py);
        let factory = pp_ref.factory.clone_ref(py);
        let dispatcher = pp_ref.dispatcher.clone();
        let role = pp_ref.interpreter_role.clone();
        // If the user constructed Props with the default role, inherit
        // the parent's role for cache locality. Otherwise honor override.
        let interpreter_role = if role == "default" { self.interpreter_role.clone() } else { role };
        drop(pp_ref);

        let (tx, rx) = oneshot::channel();
        self.send_op(CtxOp::Spawn { factory, name, interpreter_role, dispatcher, reply: tx })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match rx.await {
                Ok(Ok(actor_ref)) => Python::with_gil(|py| Py::new(py, actor_ref).map(|p| p.into_any())),
                Ok(Err(e)) => Err(PyRuntimeError::new_err(e)),
                Err(_) => Err(PyRuntimeError::new_err("spawn reply channel dropped")),
            }
        })
    }

    /// Stop a child actor by name.
    fn stop_child(&self, name: String) -> PyResult<()> {
        self.send_op(CtxOp::StopChild(name))
    }

    /// Stop self after the current message completes.
    fn stop_self(&self) -> PyResult<()> {
        self.send_op(CtxOp::StopSelf)
    }

    /// Stash the current message for a later `unstash_all`.
    fn stash(&self, msg: Py<PyAny>) -> PyResult<()> {
        self.send_op(CtxOp::Stash(msg))
    }

    /// Drain the stash back into the mailbox.
    fn unstash_all(&self) -> PyResult<()> {
        self.send_op(CtxOp::UnstashAll)
    }

    /// Set the receive-idle timeout (None to clear).
    #[pyo3(signature = (seconds=None))]
    fn set_receive_timeout(&self, seconds: Option<f64>) -> PyResult<()> {
        let d = seconds.map(Duration::from_secs_f64);
        self.send_op(CtxOp::SetReceiveTimeout(d))
    }

    /// Schedule `msg` to be delivered after `delay_secs`.
    /// If `target` is omitted, the message goes to self.
    #[pyo3(signature = (delay_secs, msg, target=None))]
    fn schedule_once(
        &self,
        py: Python<'_>,
        delay_secs: f64,
        msg: Py<PyAny>,
        target: Option<Py<PyActorRef>>,
    ) -> PyResult<()> {
        let target_inner = target.map(|t| t.borrow(py).inner.clone());
        self.send_op(CtxOp::ScheduleOnce {
            delay: Duration::from_secs_f64(delay_secs),
            msg,
            target: target_inner,
        })
    }

    /// Schedule `msg` periodically.
    #[pyo3(signature = (initial_secs, interval_secs, msg, target=None))]
    fn schedule_periodically(
        &self,
        py: Python<'_>,
        initial_secs: f64,
        interval_secs: f64,
        msg: Py<PyAny>,
        target: Option<Py<PyActorRef>>,
    ) -> PyResult<()> {
        let target_inner = target.map(|t| t.borrow(py).inner.clone());
        self.send_op(CtxOp::SchedulePeriodic {
            initial: Duration::from_secs_f64(initial_secs),
            interval: Duration::from_secs_f64(interval_secs),
            msg,
            target: target_inner,
        })
    }

    /// Replace the active handler with a new async callable
    /// `(ctx, msg) -> Awaitable`. Subsequent messages dispatch to
    /// `new_handler` instead of the actor's `handle` method.
    fn become_(&self, new_handler: Py<PyAny>) -> PyResult<()> {
        self.send_op(CtxOp::Become(new_handler))
    }

    /// Restore the actor's default handler.
    fn unbecome(&self) -> PyResult<()> {
        self.send_op(CtxOp::Unbecome)
    }

    /// Watch `target` for termination. When `target` stops, this actor
    /// receives a Python `atomr.Terminated(path)` user-message in its
    /// regular `handle(ctx, msg)` flow.
    fn watch(&self, py: Python<'_>, target: Py<PyActorRef>) -> PyResult<()> {
        let inner = target.borrow(py).inner.clone();
        self.send_op(CtxOp::Watch(inner))
    }

    /// Stop watching `target`.
    fn unwatch(&self, py: Python<'_>, target: Py<PyActorRef>) -> PyResult<()> {
        let inner = target.borrow(py).inner.clone();
        self.send_op(CtxOp::Unwatch(inner))
    }

    fn __repr__(&self) -> String {
        format!("<Context path={} sender={:?}>", self.path, self.sender.is_some())
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyContext>()
}
