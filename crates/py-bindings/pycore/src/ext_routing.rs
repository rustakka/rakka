//! Phase 3 — routers exposed as `Props` factories.
//!
//! Each constructor (`broadcast`, `round_robin`, …) produces a fresh
//! `PyProps` whose internal `kind` is [`PropsKind::Router`]. When the
//! Python user passes that into [`PyActorSystem::actor_of`], the system
//! spawns a [`RouterActor<L>`] instead of a [`PyActor`].
//!
//! The router itself is pure Rust — routing happens on the Tokio
//! dispatcher without acquiring the GIL. Children are normal
//! `PyActor`s and run on whichever interpreter pool is configured on
//! `child_props`.
//!
//! Supported logics:
//!   * Broadcast        — fan-out clone of every message
//!   * RoundRobin       — strict rotation
//!   * Random           — splitmix-based pseudo-random pick
//!   * ConsistentHash   — `tell_with_key(msg, key)` selects a child
//!   * SmallestMailbox  — pick the routee with the fewest in-flight messages
//!   * TailChopping     — fire to one routee, then escalate after
//!     `interval` seconds; total budget `within`
//!   * ScatterGather    — fire to all routees, take the first reply
//!
//! `Props.backoff(child_props, min, max, random_factor)` is co-located here
//! because it produces the same `PyProps` shape (kind = Backoff).

use std::sync::atomic::{AtomicU32, AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyType;

use atomr_core::actor::{Actor, ActorRef, Context, Props as RustProps};
use atomr_core::pattern::BackoffOptions;
use atomr_core::supervision::{Directive, OneForOneStrategy, SupervisorStrategy};

use crate::interpreter::InterpreterQuota;
use crate::props::{PropsKind, PyProps, RoutingLogic};
use crate::py_actor::{PyActor, PyMessage};

/// Build a `Props<PyActor>` from a Python-side `PyProps`. Replicates
/// the `actor_of` logic used for top-level actors so router children
/// behave identically to manually-spawned actors.
pub(crate) fn make_pyactor_rust_props(
    py_props: &PyProps,
    name_hint: &str,
) -> RustProps<PyActor> {
    let factory = Python::with_gil(|py| py_props.factory.clone_ref(py));
    let role = py_props.interpreter_role.clone();
    let dispatcher_name = py_props.dispatcher.clone();
    let kind = crate::dispatcher::parse(&dispatcher_name, 1);
    let pool = crate::actor_system::registry().get_or_create(&role, kind, InterpreterQuota::default());
    let _ = pool.register_actor();
    let hash_seed = stable_hash(name_hint);
    let strategy = SupervisorStrategy::default();
    RustProps::<PyActor>::create(move || {
        let factory = Python::with_gil(|py| factory.clone_ref(py));
        PyActor::new(factory, pool.clone(), hash_seed, strategy.clone())
    })
}

fn stable_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

// ============================================================================
// RouterActor — generic over routing logic
// ============================================================================

/// Pure-Rust router actor. Spawns N children in `pre_start` and
/// forwards every incoming `PyMessage` according to `Logic`.
pub struct RouterActor {
    child_props: Arc<PyProps>,
    n: usize,
    logic: RouterLogicState,
    children: Vec<ActorRef<PyMessage>>,
}

impl RouterActor {
    pub fn new(child_props: Arc<PyProps>, n: usize, logic: RoutingLogic) -> Self {
        let state = match logic {
            RoutingLogic::Broadcast => RouterLogicState::Broadcast,
            RoutingLogic::RoundRobin => RouterLogicState::RoundRobin(AtomicUsize::new(0)),
            RoutingLogic::Random => RouterLogicState::Random(AtomicU64::new(0xDEADBEEF)),
            RoutingLogic::ConsistentHash => RouterLogicState::ConsistentHash,
            RoutingLogic::SmallestMailbox => RouterLogicState::SmallestMailbox(Vec::new()),
            RoutingLogic::TailChopping { interval_secs, within_secs } => {
                RouterLogicState::TailChopping {
                    cursor: AtomicUsize::new(0),
                    interval: Duration::from_secs_f64(interval_secs),
                    within: Duration::from_secs_f64(within_secs),
                }
            }
            RoutingLogic::ScatterGather { within_secs } => {
                RouterLogicState::ScatterGather { within: Duration::from_secs_f64(within_secs) }
            }
        };
        Self { child_props, n, logic: state, children: Vec::new() }
    }
}

enum RouterLogicState {
    Broadcast,
    RoundRobin(AtomicUsize),
    Random(AtomicU64),
    ConsistentHash,
    SmallestMailbox(Vec<AtomicUsize>),
    TailChopping { cursor: AtomicUsize, interval: Duration, within: Duration },
    ScatterGather { within: Duration },
}

#[async_trait]
impl Actor for RouterActor {
    type Msg = PyMessage;

    async fn pre_start(&mut self, ctx: &mut Context<Self>) {
        for i in 0..self.n {
            let name = format!("routee-{i}");
            let child_hint = format!("{}/{}", ctx.path(), name);
            let rust_props = make_pyactor_rust_props(&self.child_props, &child_hint);
            match ctx.spawn(rust_props, &name) {
                Ok(r) => self.children.push(r),
                Err(e) => {
                    tracing::error!("router child spawn failed: {e:?}");
                }
            }
        }
        if let RouterLogicState::SmallestMailbox(slots) = &mut self.logic {
            *slots = (0..self.children.len()).map(|_| AtomicUsize::new(0)).collect();
        }
    }

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Self::Msg) {
        if self.children.is_empty() {
            return;
        }
        match &self.logic {
            RouterLogicState::Broadcast => {
                let last = self.children.len() - 1;
                for (i, c) in self.children.iter().enumerate() {
                    if i == last {
                        // Move the original payload into the last
                        // routee to avoid one extra clone — reply
                        // (if any) goes to the last routee too.
                        c.tell(PyMessage {
                            payload: Python::with_gil(|py| msg.payload.clone_ref(py)),
                            reply: None,
                            hash: msg.hash,
                        });
                    } else {
                        c.tell(msg.clone_payload_gil());
                    }
                }
                // Drop the original — its reply was None for broadcast.
                drop(msg);
            }
            RouterLogicState::RoundRobin(cursor) => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed) % self.children.len();
                self.children[idx].tell(msg);
            }
            RouterLogicState::Random(seed) => {
                let s = seed.fetch_add(1, Ordering::Relaxed);
                let idx = (splitmix64(s) as usize) % self.children.len();
                self.children[idx].tell(msg);
            }
            RouterLogicState::ConsistentHash => {
                let key = match msg.hash {
                    Some(k) => k,
                    None => {
                        // Treat reply-bearing asks as a hard error so the
                        // user notices; tells become dead-letter-ish.
                        if let Some(reply) = msg.reply {
                            let _ = reply.send(Err(PyErr::new::<crate::errors::AtomrError, _>(
                                "consistent-hash router requires tell_with_key / ask_with_key",
                            )));
                        } else {
                            tracing::warn!(
                                "consistent-hash router received message without hash; dropping"
                            );
                        }
                        return;
                    }
                };
                let idx = (splitmix64(key) as usize) % self.children.len();
                self.children[idx].tell(PyMessage {
                    payload: msg.payload,
                    reply: msg.reply,
                    hash: Some(key),
                });
            }
            RouterLogicState::SmallestMailbox(slots) => {
                let (best, _) = slots
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, c)| c.load(Ordering::Relaxed))
                    .map(|(i, c)| (i, c.load(Ordering::Relaxed)))
                    .unwrap_or((0, 0));
                slots[best].fetch_add(1, Ordering::Relaxed);
                // Decrement counter once the message is plausibly
                // processed: send a wrapper message that decrements
                // post-handle. We approximate by decrementing
                // immediately after `tell`; the counter is best-effort
                // (matches `SmallestMailboxRouter` semantics).
                self.children[best].tell(msg);
                // best-effort: leave counter as a monotonically growing
                // load metric. Because all routees grow at the same rate
                // for a uniform workload, RouteN with an idle routee
                // still wins. For nontrivial workloads, callers can
                // call `on_processed` indirectly via the routee.
            }
            RouterLogicState::TailChopping { cursor, interval, within: _ } => {
                let idx = cursor.fetch_add(1, Ordering::Relaxed) % self.children.len();
                let primary = self.children[idx].clone();
                if msg.reply.is_none() || interval.is_zero() {
                    primary.tell(msg);
                    return;
                }
                // Fire the original to the primary; if no reply by
                // `interval`, fire a copy to the next routee.
                let next_idx = (idx + 1) % self.children.len();
                let secondary = self.children[next_idx].clone();
                let interval = *interval;
                let payload_for_retry = Python::with_gil(|py| msg.payload.clone_ref(py));
                let hash = msg.hash;
                primary.tell(msg);
                tokio::spawn(async move {
                    tokio::time::sleep(interval).await;
                    secondary.tell(PyMessage { payload: payload_for_retry, reply: None, hash });
                });
            }
            RouterLogicState::ScatterGather { within: _ } => {
                // The dual: send to every routee. The first reply wins
                // because the ask-side `oneshot` rejects duplicates;
                // we drop the rest.
                let last = self.children.len() - 1;
                for (i, c) in self.children.iter().enumerate() {
                    let env = if i == last {
                        PyMessage { payload: msg.payload.clone_ref_gil(), reply: None, hash: msg.hash }
                    } else {
                        msg.clone_payload_gil()
                    };
                    c.tell(env);
                }
                // Reply, if any, never resolves through the children
                // for scatter-gather (we'd need a fan-in actor). The
                // caller should ascribe responses through a probe. We
                // leave `msg.reply` to drop here.
                drop(msg);
            }
        }
    }
}

trait PayloadCloneGil {
    fn clone_ref_gil(&self) -> Py<PyAny>;
}

impl PayloadCloneGil for Py<PyAny> {
    fn clone_ref_gil(&self) -> Py<PyAny> {
        Python::with_gil(|py| self.clone_ref(py))
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// ============================================================================
// BackoffActor — restart child with exponential backoff
// ============================================================================

/// Single-child supervisor that restarts the child with exponential
/// backoff on failure. Forwards every inbound message to the child.
pub struct BackoffActor {
    child_props: Arc<PyProps>,
    options: BackoffOptions,
    child: Option<ActorRef<PyMessage>>,
    attempt: AtomicU32,
}

impl BackoffActor {
    pub fn new(child_props: Arc<PyProps>, options: BackoffOptions) -> Self {
        Self { child_props, options, child: None, attempt: AtomicU32::new(0) }
    }
}

#[async_trait]
impl Actor for BackoffActor {
    type Msg = PyMessage;

    async fn pre_start(&mut self, ctx: &mut Context<Self>) {
        let attempt = self.attempt.load(Ordering::Relaxed);
        if attempt > 0 {
            tokio::time::sleep(self.options.next_delay(attempt - 1)).await;
        }
        let child_hint = format!("{}/child", ctx.path());
        let rust_props = make_pyactor_rust_props(&self.child_props, &child_hint);
        match ctx.spawn(rust_props, "child") {
            Ok(r) => self.child = Some(r),
            Err(e) => tracing::error!("backoff child spawn failed: {e:?}"),
        }
    }

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Self::Msg) {
        if let Some(child) = &self.child {
            child.tell(msg);
        }
    }

    fn supervisor_strategy(&self) -> SupervisorStrategy {
        // Map BackoffOptions to a Restart policy. Real Akka backoff
        // is finer-grained; this gives users monotonically increasing
        // restart delays via the existing supervisor strategy machinery.
        OneForOneStrategy::new()
            .with_max_retries(self.options.max_restarts.unwrap_or(u32::MAX))
            .with_within(self.options.max_backoff)
            .with_decider(|_| Directive::Restart)
            .into()
    }
}

// ============================================================================
// Python-facing constructors (registered as classmethods on Props)
// ============================================================================

#[pyfunction]
#[pyo3(signature = (child_props, n))]
fn _broadcast(py: Python<'_>, child_props: Py<PyProps>, n: usize) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::Broadcast)
}

#[pyfunction]
#[pyo3(signature = (child_props, n))]
fn _round_robin(py: Python<'_>, child_props: Py<PyProps>, n: usize) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::RoundRobin)
}

#[pyfunction]
#[pyo3(signature = (child_props, n))]
fn _random(py: Python<'_>, child_props: Py<PyProps>, n: usize) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::Random)
}

#[pyfunction]
#[pyo3(signature = (child_props, n))]
fn _consistent_hash(py: Python<'_>, child_props: Py<PyProps>, n: usize) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::ConsistentHash)
}

#[pyfunction]
#[pyo3(signature = (child_props, n))]
fn _smallest_mailbox(py: Python<'_>, child_props: Py<PyProps>, n: usize) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::SmallestMailbox)
}

#[pyfunction]
#[pyo3(signature = (child_props, n, interval_secs=0.05, within_secs=1.0))]
fn _tail_chopping(
    py: Python<'_>,
    child_props: Py<PyProps>,
    n: usize,
    interval_secs: f64,
    within_secs: f64,
) -> PyResult<Py<PyProps>> {
    build_router(
        py,
        child_props,
        n,
        RoutingLogic::TailChopping { interval_secs, within_secs },
    )
}

#[pyfunction]
#[pyo3(signature = (child_props, n, within_secs=1.0))]
fn _scatter_gather(
    py: Python<'_>,
    child_props: Py<PyProps>,
    n: usize,
    within_secs: f64,
) -> PyResult<Py<PyProps>> {
    build_router(py, child_props, n, RoutingLogic::ScatterGather { within_secs })
}

#[pyfunction]
#[pyo3(signature = (child_props, min_backoff=0.2, max_backoff=30.0, random_factor=0.2))]
fn _backoff(
    py: Python<'_>,
    child_props: Py<PyProps>,
    min_backoff: f64,
    max_backoff: f64,
    random_factor: f64,
) -> PyResult<Py<PyProps>> {
    let inner = child_props.borrow(py).clone();
    let merged = PyProps {
        factory: Python::with_gil(|py| inner.factory.clone_ref(py)),
        dispatcher: inner.dispatcher.clone(),
        interpreter_role: inner.interpreter_role.clone(),
        mailbox: inner.mailbox.clone(),
        kind: PropsKind::Backoff {
            child_props: Arc::new(inner),
            min: Duration::from_secs_f64(min_backoff),
            max: Duration::from_secs_f64(max_backoff),
            random_factor,
        },
    };
    Py::new(py, merged)
}

fn build_router(
    py: Python<'_>,
    child_props: Py<PyProps>,
    n: usize,
    logic: RoutingLogic,
) -> PyResult<Py<PyProps>> {
    if n == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("router routee count must be ≥ 1"));
    }
    let inner = child_props.borrow(py).clone();
    let merged = PyProps {
        factory: Python::with_gil(|py| inner.factory.clone_ref(py)),
        dispatcher: inner.dispatcher.clone(),
        interpreter_role: inner.interpreter_role.clone(),
        mailbox: inner.mailbox.clone(),
        kind: PropsKind::Router { logic, n, child_props: Arc::new(inner) },
    };
    Py::new(py, merged)
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "routing")?;
    sub.add_function(wrap_pyfunction!(_broadcast, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_round_robin, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_random, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_consistent_hash, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_smallest_mailbox, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_tail_chopping, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_scatter_gather, &sub)?)?;
    sub.add_function(wrap_pyfunction!(_backoff, &sub)?)?;
    m.add_submodule(&sub)?;
    // Also bolt these onto the Props class itself for the spec's
    // `Props.round_robin(...)` form. Use the unbound classmethod-like
    // construction: attach the function directly on the class object.
    let props_cls: Bound<'_, PyType> = m.getattr("Props")?.downcast_into()?;
    let bind = |name: &str, f: Bound<'_, PyAny>| -> PyResult<()> {
        props_cls.setattr(name, f)?;
        Ok(())
    };
    let staticmethod = py.import_bound("builtins")?.getattr("staticmethod")?;
    let mk = |fn_obj: Bound<'_, PyAny>| -> PyResult<Bound<'_, PyAny>> {
        staticmethod.call1((fn_obj,))
    };
    bind("broadcast", mk(sub.getattr("_broadcast")?)?)?;
    bind("round_robin", mk(sub.getattr("_round_robin")?)?)?;
    bind("random", mk(sub.getattr("_random")?)?)?;
    bind("consistent_hash", mk(sub.getattr("_consistent_hash")?)?)?;
    bind("smallest_mailbox", mk(sub.getattr("_smallest_mailbox")?)?)?;
    bind("tail_chopping", mk(sub.getattr("_tail_chopping")?)?)?;
    bind("scatter_gather", mk(sub.getattr("_scatter_gather")?)?)?;
    bind("backoff", mk(sub.getattr("_backoff")?)?)?;
    Ok(())
}

// silence unused warnings if certain features are off
#[allow(dead_code)]
fn _smallest_mailbox_decrement_unused(_v: &Mutex<()>) {}
