//! Bridge actor: a Rust `Actor` that forwards every call to a Python
//! `Actor` subclass instance. Each Python actor spawned through the
//! binding layer instantiates one of these.
//!
//! Design summary:
//!   * The Rust `handle` is `async` and runs on the actor's Tokio
//!     dispatcher. We do **not** take the GIL inside `handle`. Instead we
//!     hand the message to the actor's assigned `InterpreterInstance` via
//!     a task channel and await completion on a `oneshot`. This keeps the
//!     Rust mailbox lock-free regardless of GIL contention.
//!   * Python exceptions are captured and re-raised as Rust panics so the
//!     existing supervision strategy can act on them identically to native
//!     Rust panics.
//!   * The Python instance is created exactly once; restarts reuse the
//!     same factory callable (stored in `PyProps`) to produce a fresh
//!     instance.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::sync::oneshot;

use rustakka_core::actor::{Actor, Context};
use rustakka_core::supervision::SupervisorStrategy;

use crate::interpreter::{InterpreterInstance, PyTask};

/// Erased Python message — we wrap `Py<PyAny>` plus an optional reply
/// channel. The reply channel is used by `ask`.
pub struct PyMessage {
    pub payload: Py<PyAny>,
    pub reply: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

impl PyMessage {
    pub fn new(payload: Py<PyAny>) -> Self {
        Self { payload, reply: None }
    }

    pub fn ask(payload: Py<PyAny>) -> (Self, oneshot::Receiver<PyResult<Py<PyAny>>>) {
        let (tx, rx) = oneshot::channel();
        (Self { payload, reply: Some(tx) }, rx)
    }
}

pub struct PyActor {
    pub(crate) instance: Option<Py<PyAny>>,
    pub(crate) factory: Py<PyAny>,
    pub(crate) pool: Arc<InterpreterInstance>,
    pub(crate) hash_seed: u64,
    pub(crate) strategy: SupervisorStrategy,
}

impl PyActor {
    pub fn new(
        factory: Py<PyAny>,
        pool: Arc<InterpreterInstance>,
        hash_seed: u64,
        strategy: SupervisorStrategy,
    ) -> Self {
        Self { instance: None, factory, pool, hash_seed, strategy }
    }

    fn worker(&self) -> Arc<crate::interpreter::Worker> {
        self.pool.worker_for(self.hash_seed)
    }

    /// Execute `f` on the actor's assigned interpreter and wait for the
    /// result. Records GIL-hold duration into the interpreter metrics.
    async fn on_interpreter<F, R>(&self, f: F) -> PyResult<R>
    where
        F: for<'py> FnOnce(Python<'py>, Option<&Py<PyAny>>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let worker = self.worker();
        let instance = self.instance.as_ref().map(|p| p.clone_ref_py());
        let pool = self.pool.clone();
        let task = PyTask {
            run: Box::new(move |py| {
                let t0 = Instant::now();
                let res = f(py, instance.as_ref());
                let dt = t0.elapsed().as_nanos() as u64;
                pool.metrics
                    .gil_hold_ns_total
                    .fetch_add(dt, std::sync::atomic::Ordering::Relaxed);
                pool.metrics
                    .messages_handled
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(max) = pool.quota.max_handler_ms {
                    if dt / 1_000_000 > max {
                        pool.metrics
                            .long_handlers
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            pool = %pool.label,
                            dt_ns = dt,
                            max_ms = max,
                            "python handler exceeded max_handler_ms"
                        );
                    }
                }
                let _ = tx.send(res);
            }),
        };
        if worker.tx.send(task).is_err() {
            return Err(PyErr::new::<crate::errors::RustakkaError, _>(
                "interpreter worker shut down",
            ));
        }
        rx.await.unwrap_or_else(|_| {
            Err(PyErr::new::<crate::errors::RustakkaError, _>(
                "interpreter worker dropped task",
            ))
        })
    }
}

// Helper: `Py::clone_ref` needs `Python<'_>`. We define a tiny extension so
// we can duplicate handles from non-GIL code paths by acquiring briefly.
trait PyCloneRef {
    fn clone_ref_py(&self) -> Py<PyAny>;
}

impl PyCloneRef for Py<PyAny> {
    fn clone_ref_py(&self) -> Py<PyAny> {
        Python::with_gil(|py| self.clone_ref(py))
    }
}

#[async_trait]
impl Actor for PyActor {
    type Msg = PyMessage;

    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {
        let factory = self.factory.clone_ref_py();
        let res = self
            .on_interpreter(move |py, _| {
                let instance = factory.call0(py)?;
                Ok::<Py<PyAny>, PyErr>(instance)
            })
            .await;
        match res {
            Ok(instance) => {
                self.instance = Some(instance.clone_ref_py());
                // Optional pre_start hook.
                let inst = instance;
                let _ = self
                    .on_interpreter(move |py, _| {
                        if let Ok(hook) = inst.bind(py).getattr("pre_start") {
                            if !hook.is_none() {
                                let args = PyTuple::new_bound(py, &[py.None()]);
                                let res = hook.call1(args)?;
                                coro_run(py, res)?;
                            }
                        }
                        Ok(())
                    })
                    .await;
            }
            Err(e) => {
                panic!("python actor factory raised: {}", e);
            }
        }
    }

    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {
        if let Some(instance) = self.instance.take() {
            let _ = self
                .on_interpreter(move |py, _| {
                    if let Ok(hook) = instance.bind(py).getattr("post_stop") {
                        if !hook.is_none() {
                            let args = PyTuple::new_bound(py, &[py.None()]);
                            let res = hook.call1(args)?;
                            coro_run(py, res)?;
                        }
                    }
                    Ok(())
                })
                .await;
            self.pool.unregister_actor();
        }
    }

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Self::Msg) {
        let PyMessage { payload, reply } = msg;
        let Some(instance) = self.instance.as_ref().map(|p| p.clone_ref_py()) else {
            return;
        };
        let result = self
            .on_interpreter(move |py, _| {
                let handle = instance.bind(py).getattr("handle")?;
                let args = PyTuple::new_bound(py, &[py.None().into_any(), payload.into_any()]);
                let res = handle.call1(args)?;
                coro_run(py, res)
            })
            .await;

        if let Some(tx) = reply {
            let _ = tx.send(result);
        } else if let Err(e) = result {
            self.pool
                .metrics
                .handler_panics
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Surface the error through supervision.
            panic!(
                "python actor handler raised: {}",
                Python::with_gil(|py| format!("{}", e.value_bound(py)))
            );
        }
    }

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        self.strategy.clone()
    }
}

/// Run a returned Python value; if it's a coroutine, await it on the
/// interpreter's asyncio event loop synchronously, otherwise return as is.
fn coro_run<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    let asyncio = py.import_bound("asyncio")?;
    let is_coro: bool =
        asyncio.call_method1("iscoroutine", (value.clone(),))?.extract().unwrap_or(false);
    if is_coro {
        // Run the coroutine to completion on a temporary event loop. The
        // Python handler is expected to be short-lived; for long async
        // work the user should spawn a task on the worker's loop.
        let new_loop = asyncio.call_method0("new_event_loop")?;
        let res = new_loop.call_method1("run_until_complete", (value,))?;
        new_loop.call_method0("close")?;
        Ok(res.unbind())
    } else {
        Ok(value.unbind())
    }
}

