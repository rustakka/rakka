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
//!   * Phase 1 — Real `Context`. Each handler call gets a fresh
//!     `PyContext` `#[pyclass]` carrying value-typed snapshots and an
//!     `mpsc::UnboundedSender<CtxOp>`. We drain that channel during the
//!     coroutine's await window via `tokio::select!` so that
//!     `ctx.spawn(...)` can resolve before the handler returns.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::sync::{mpsc, oneshot};

use atomr_core::actor::{Actor, ActorPath, ActorRef as RustRef, Context};
use atomr_core::supervision::{PanicPayload, SupervisorStrategy};

use crate::actor_ref::PyActorRef;
use crate::context::{CtxOp, PyContext};
use crate::dispatcher;
use crate::interpreter::{InterpreterInstance, InterpreterQuota, PyTask};

/// Erased Python message — we wrap `Py<PyAny>` plus an optional reply
/// channel. The reply channel is used by `ask`. `sender_ref` carries
/// the typed `ActorRef<PyMessage>` of the sender so `ctx.sender` can
/// surface a usable handle (Phase 1). `hash` is an optional consistent-hash
/// key used by the Phase-3 consistent-hash router.
pub struct PyMessage {
    pub payload: Py<PyAny>,
    pub reply: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
    pub sender_ref: Option<Arc<RustRef<PyMessage>>>,
    pub hash: Option<u64>,
}

impl PyMessage {
    pub fn new(payload: Py<PyAny>) -> Self {
        Self { payload, reply: None, sender_ref: None, hash: None }
    }

    pub fn with_sender(payload: Py<PyAny>, sender: Arc<RustRef<PyMessage>>) -> Self {
        Self { payload, reply: None, sender_ref: Some(sender), hash: None }
    }

    /// New message envelope carrying an explicit consistent-hash key.
    pub fn with_hash(payload: Py<PyAny>, hash: u64) -> Self {
        Self { payload, reply: None, sender_ref: None, hash: Some(hash) }
    }

    pub fn ask(payload: Py<PyAny>) -> (Self, oneshot::Receiver<PyResult<Py<PyAny>>>) {
        let (tx, rx) = oneshot::channel();
        (Self { payload, reply: Some(tx), sender_ref: None, hash: None }, rx)
    }

    /// `ask` variant carrying a consistent-hash key.
    pub fn ask_with_hash(
        payload: Py<PyAny>,
        hash: u64,
    ) -> (Self, oneshot::Receiver<PyResult<Py<PyAny>>>) {
        let (tx, rx) = oneshot::channel();
        (Self { payload, reply: Some(tx), sender_ref: None, hash: Some(hash) }, rx)
    }

    /// Best-effort clone of the payload using the GIL. Reply channels
    /// are *not* duplicated — the clone is reply-less. Used by
    /// broadcast routing.
    pub fn clone_payload_gil(&self) -> Self {
        let payload = Python::with_gil(|py| self.payload.clone_ref(py));
        Self { payload, reply: None, sender_ref: self.sender_ref.clone(), hash: self.hash }
    }
}

pub struct PyActor {
    pub(crate) instance: Option<Py<PyAny>>,
    pub(crate) factory: Py<PyAny>,
    pub(crate) pool: Arc<InterpreterInstance>,
    pub(crate) hash_seed: u64,
    pub(crate) strategy: SupervisorStrategy,
    /// If set, dispatch to this callable instead of `instance.handle`.
    /// Toggled by `ctx.become(new_handler)` / `ctx.unbecome()`.
    pub(crate) current_handler: Option<Py<PyAny>>,
    /// Set of paths this actor is watching. Populated from `CtxOp::Watch`
    /// and consulted in `on_terminated` to translate the framework
    /// `Terminated(path)` system message into a Python-visible
    /// `atomr.Terminated(path)` user message tell-ed to self.
    pub(crate) watching: HashSet<String>,
}

impl PyActor {
    pub fn new(
        factory: Py<PyAny>,
        pool: Arc<InterpreterInstance>,
        hash_seed: u64,
        strategy: SupervisorStrategy,
    ) -> Self {
        Self {
            instance: None,
            factory,
            pool,
            hash_seed,
            strategy,
            current_handler: None,
            watching: HashSet::new(),
        }
    }

    /// Interpreter role used when spawning children — inherited from the
    /// pool label this actor was assigned to.
    pub(crate) fn interpreter_role(&self) -> &str {
        &self.pool.label
    }

    /// Default dispatcher name for child actors when their props don't
    /// override it.
    pub(crate) fn dispatcher_name(&self) -> &'static str {
        "python-pinned"
    }

    fn worker(&self) -> Arc<crate::interpreter::Worker> {
        self.pool.worker_for(self.hash_seed)
    }

    /// Run the factory closure and store the fresh Python instance,
    /// then call its optional `pre_start` hook. Used by both
    /// `Actor::pre_start` and `Actor::post_restart`.
    async fn build_instance(&mut self) {
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

    /// Execute `f` on the actor's assigned interpreter and wait for the
    /// result. Records GIL-hold duration into the interpreter metrics.
    async fn on_interpreter<F, R>(&self, f: F) -> PyResult<R>
    where
        F: for<'py> FnOnce(Python<'py>, Option<&Py<PyAny>>) -> PyResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let worker = self.worker();
        let instance = self.instance.as_ref().map(|p| p.clone_ref_py());
        let pool = self.pool.clone();
        run_on_interpreter(worker, pool, instance, f).await
    }
}

/// Free-function variant of `on_interpreter` so we can dispatch a call
/// to the interpreter and `tokio::select!` on the result future without
/// holding a borrow on `&self` (which would block us from also draining
/// `CtxOp`s during the await window).
fn run_on_interpreter<F, R>(
    worker: Arc<crate::interpreter::Worker>,
    pool: Arc<InterpreterInstance>,
    instance: Option<Py<PyAny>>,
    f: F,
) -> impl std::future::Future<Output = PyResult<R>> + Send + 'static
where
    F: for<'py> FnOnce(Python<'py>, Option<&Py<PyAny>>) -> PyResult<R> + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let task = PyTask {
        run: Box::new(move |py| {
            let t0 = Instant::now();
            let res = f(py, instance.as_ref());
            let dt = t0.elapsed().as_nanos() as u64;
            pool.metrics.gil_hold_ns_total.fetch_add(dt, std::sync::atomic::Ordering::Relaxed);
            pool.metrics.messages_handled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(max) = pool.quota.max_handler_ms {
                if dt / 1_000_000 > max {
                    pool.metrics.long_handlers.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    let send_result = worker.tx.send(task);
    async move {
        if send_result.is_err() {
            return Err(PyErr::new::<crate::errors::AtomrError, _>("interpreter worker shut down"));
        }
        rx.await.unwrap_or_else(|_| {
            Err(PyErr::new::<crate::errors::AtomrError, _>("interpreter worker dropped task"))
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
        self.build_instance().await;
    }

    async fn post_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {
        // After a supervisor-driven restart, the cell has just swapped
        // `*actor = props.new_actor()` so our `instance` slot is empty.
        // Rebuild the Python instance and re-run its `pre_start` hook
        // so user code observes a clean lifecycle each time.
        self.build_instance().await;
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

    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg) {
        let PyMessage { payload, reply, sender_ref, hash: _ } = msg;
        let Some(instance) = self.instance.as_ref().map(|p| p.clone_ref_py()) else {
            return;
        };

        // Snapshot context fields for the Python side.
        let path_str = ctx.path().to_string();
        let self_ref_arc = Arc::new(ctx.self_ref().clone());
        let self_ref_py = Python::with_gil(|py| {
            Py::new(
                py,
                PyActorRef::from_arc(self_ref_arc.clone(), path_str.clone()),
            )
        });
        let self_ref_py = match self_ref_py {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to create self PyActorRef: {}", e);
                return;
            }
        };

        // Surface sender as a usable PyActorRef when available.
        let sender_py = if let Some(sender_arc) = sender_ref {
            let sender_path = sender_arc.path().to_string();
            Python::with_gil(|py| {
                Py::new(py, PyActorRef::from_arc(sender_arc, sender_path)).ok()
            })
        } else {
            None
        };
        // Op channel for the duration of this handler call.
        let (op_tx, mut op_rx) = mpsc::unbounded_channel::<CtxOp>();

        let py_ctx = Python::with_gil(|py| {
            Py::new(
                py,
                PyContext::new(
                    self_ref_py.clone_ref(py),
                    path_str.clone(),
                    sender_py.as_ref().map(|s| s.clone_ref(py)),
                    self.interpreter_role().to_string(),
                    self.dispatcher_name().to_string(),
                    op_tx.clone(),
                ),
            )
        });
        let py_ctx = match py_ctx {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("failed to create PyContext: {}", e);
                return;
            }
        };

        // Decide which Python callable to invoke: the optional
        // become-handler or the actor's `handle` method.
        let handler = self.current_handler.as_ref().map(|p| p.clone_ref_py());
        let py_ctx_for_call = Python::with_gil(|py| py_ctx.clone_ref(py));
        let payload_for_call = payload;

        // Spawn the interpreter call on the worker, then race its
        // completion against the op channel so `ctx.spawn` can resolve
        // mid-handler. We call the free `run_on_interpreter` so the
        // future does not borrow `self` — that lets us mutate `self`
        // and `ctx` from `apply_op_eager` while the call is in flight.
        let worker = self.worker();
        let pool = self.pool.clone();
        let instance_for_call = Some(instance.clone_ref_py());
        let result_fut = run_on_interpreter::<_, Py<PyAny>>(
            worker,
            pool,
            instance_for_call,
            move |py, inst_opt| {
                let ctx_arg = py_ctx_for_call.into_any();
                let args = PyTuple::new_bound(py, &[ctx_arg.bind(py).clone(), payload_for_call.bind(py).clone()]);
                let res = if let Some(handler) = handler {
                    handler.call1(py, args)?
                } else {
                    let inst = inst_opt.expect("instance set before handle");
                    let h = inst.bind(py).getattr("handle")?;
                    h.call1(args)?.unbind()
                };
                coro_run(py, res.bind(py).clone())
            },
        );

        tokio::pin!(result_fut);

        let handler_result: PyResult<Py<PyAny>>;
        loop {
            tokio::select! {
                biased;
                op = op_rx.recv() => {
                    match op {
                        Some(op) => apply_op_eager(self, ctx, op).await,
                        None => {
                            // Channel closed but handler not done -
                            // shouldn't happen since we hold the sender.
                        }
                    }
                }
                res = &mut result_fut => {
                    handler_result = res;
                    break;
                }
            }
        }

        // Invalidate the op channel so any leaked PyContext refs
        // can't push more ops, then drain anything still pending.
        Python::with_gil(|py| py_ctx.borrow(py).invalidate());
        drop(op_tx);
        while let Ok(op) = op_rx.try_recv() {
            apply_op_eager(self, ctx, op).await;
        }

        let result = handler_result;

        if let Some(tx) = reply {
            let _ = tx.send(result);
        } else if let Err(e) = result {
            self.pool.metrics.handler_panics.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Surface the error through supervision via a structured
            // payload so deciders can match on the Python class.
            let payload = Python::with_gil(|py| extract_panic_payload(py, &e));
            std::panic::panic_any(payload);
        }
    }

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        self.strategy.clone()
    }

    async fn on_terminated(&mut self, ctx: &mut Context<Self>, path: &ActorPath) {
        let path_str = path.to_string();
        if !self.watching.remove(&path_str) {
            // We weren't tracking this path through Python — nothing
            // to deliver. (E.g., the user installed a watch via some
            // future Rust-only path.)
            return;
        }
        // Build a Python `atomr.Terminated(path=...)` instance and
        // tell it to self as a regular user message so `handle` sees
        // it through the normal dispatch flow.
        let payload = Python::with_gil(|py| build_terminated_message(py, &path_str));
        let payload = match payload {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    path = %path_str,
                    "failed to build Terminated payload: {}",
                    Python::with_gil(|py| format!("{}", e.value_bound(py)))
                );
                return;
            }
        };
        ctx.self_ref().tell(PyMessage::new(payload));
    }
}

/// Materialize a structured panic payload from a Python exception so
/// that the supervisor decider can classify failures by class.
fn extract_panic_payload(py: Python<'_>, e: &PyErr) -> PanicPayload {
    let value = e.value_bound(py);
    let cls = match value.get_type().getattr("__module__") {
        Ok(m) => m.extract::<String>().unwrap_or_else(|_| "builtins".into()),
        Err(_) => "builtins".into(),
    };
    let qualname = match value.get_type().getattr("__qualname__") {
        Ok(q) => q.extract::<String>().ok(),
        Err(_) => None,
    }
    .or_else(|| value.get_type().name().ok().map(|n| n.to_string()))
    .unwrap_or_else(|| "Exception".into());
    let repr = format!("{}", value);
    PanicPayload::new(cls, qualname, repr)
}

/// Build the `atomr.Terminated(path=...)` Python value used to deliver
/// watch notifications to a Python actor. We import the Python module
/// lazily and fall back to a plain `dict` if the import fails (e.g.
/// during early startup or in stripped-down embeddings).
fn build_terminated_message(py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
    if let Ok(m) = py.import_bound("atomr") {
        if let Ok(t) = m.getattr("Terminated") {
            let kwargs = pyo3::types::PyDict::new_bound(py);
            kwargs.set_item("path", path)?;
            let v = t.call((), Some(&kwargs))?;
            return Ok(v.unbind());
        }
    }
    // Fallback: plain dict so user code still observes the event.
    let dict = pyo3::types::PyDict::new_bound(py);
    dict.set_item("__terminated__", true)?;
    dict.set_item("path", path)?;
    Ok(dict.into_any().unbind())
}

/// Apply a `CtxOp` against the live `&mut Context<PyActor>`.
async fn apply_op_eager(actor: &mut PyActor, ctx: &mut Context<PyActor>, op: CtxOp) {
    use atomr_core::actor::Props as RustProps;
    match op {
        CtxOp::Spawn { factory, name, interpreter_role, dispatcher, reply } => {
            let kind = dispatcher::parse(&dispatcher, 1);
            let pool = crate::actor_system::registry().get_or_create(
                &interpreter_role,
                kind,
                InterpreterQuota::default(),
            );
            if let Err(e) = pool.register_actor() {
                let msg = Python::with_gil(|py| format!("{}", e.value_bound(py)));
                let _ = reply.send(Err(msg));
                return;
            }

            let path_str = format!("{}/{}", ctx.path(), name);
            let hash_seed = stable_hash(&path_str);
            let strategy = actor.strategy.clone();
            let strategy_for_actor = strategy.clone();
            let factory_for_actor = factory;
            let pool_cl = pool.clone();
            let _ = dispatcher; // role lives on the pool; dispatcher is captured by pool kind

            let rust_props = RustProps::<PyActor>::create(move || {
                let factory = Python::with_gil(|py| factory_for_actor.clone_ref(py));
                PyActor::new(
                    factory,
                    pool_cl.clone(),
                    hash_seed,
                    strategy_for_actor.clone(),
                )
            })
            .with_supervisor_strategy(strategy);

            match ctx.spawn(rust_props, &name) {
                Ok(child_ref) => {
                    let py_ref = PyActorRef::from_arc(Arc::new(child_ref), path_str);
                    let _ = reply.send(Ok(py_ref));
                }
                Err(e) => {
                    let _ = reply.send(Err(e.to_string()));
                }
            }
        }
        CtxOp::StopChild(name) => {
            ctx.stop_child(&name);
        }
        CtxOp::Stash(msg) => {
            ctx.stash(PyMessage::new(msg));
        }
        CtxOp::UnstashAll => {
            let drained = ctx.unstash_all();
            for m in drained {
                ctx.self_ref().tell(m);
            }
        }
        CtxOp::StopSelf => {
            ctx.stop_self();
        }
        CtxOp::SetReceiveTimeout(d) => {
            ctx.set_receive_timeout(d);
        }
        CtxOp::ScheduleOnce { delay, msg, target } => {
            let target = target.unwrap_or_else(|| Arc::new(ctx.self_ref().clone()));
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                target.tell(PyMessage::new(msg));
            });
        }
        CtxOp::SchedulePeriodic { initial, interval, msg, target } => {
            let target = target.unwrap_or_else(|| Arc::new(ctx.self_ref().clone()));
            let msg_holder = Arc::new(parking_lot::Mutex::new(msg));
            tokio::spawn(async move {
                tokio::time::sleep(initial).await;
                let mut tick = tokio::time::interval(interval);
                tick.tick().await; // first tick fires immediately - skip
                loop {
                    let m = Python::with_gil(|py| msg_holder.lock().clone_ref(py));
                    target.tell(PyMessage::new(m));
                    tick.tick().await;
                }
            });
        }
        CtxOp::Become(handler) => {
            actor.current_handler = Some(handler);
        }
        CtxOp::Unbecome => {
            actor.current_handler = None;
        }
        CtxOp::Watch(target) => {
            // Track on the PyActor so that `on_terminated` knows which
            // watches were initiated from Python (and we can synthesize
            // a Python-visible Terminated message).
            let target_path = target.path().to_string();
            actor.watching.insert(target_path);
            ctx.watch(target.as_ref());
        }
        CtxOp::Unwatch(target) => {
            let target_path = target.path().to_string();
            actor.watching.remove(&target_path);
            ctx.unwatch(target.as_ref());
        }
    }
}

fn stable_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

/// Run a returned Python value; if it's a coroutine, await it on the
/// interpreter's asyncio event loop synchronously, otherwise return as is.
fn coro_run<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Py<PyAny>> {
    let asyncio = py.import_bound("asyncio")?;
    let is_coro: bool = asyncio.call_method1("iscoroutine", (value.clone(),))?.extract().unwrap_or(false);
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
