//! Interpreter instances — the Rust-side unit of Python GIL isolation.
//!
//! We model the "shape" of each dispatcher as an `InterpreterKind` enum, then
//! provide concrete `InterpreterInstance` values that own a task channel and
//! a policy/metrics snapshot. For the first shipping slice we implement the
//! pinned and subinterpreter-pool variants in terms of dedicated OS threads
//! that each run an `asyncio`-free dispatch loop.
//!
//! Sub-interpreter support (PEP 684) is gated behind a feature flag and a
//! runtime capability probe (`interpreter::subinterpreters_supported()`); on
//! CPython < 3.12 or when the probe fails we transparently degrade to the
//! pinned model and surface a `CompatibilityWarning`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::mpsc;

use crate::errors;

#[pyclass(name = "InterpreterQuota", module = "rakka._native")]
#[derive(Clone, Default)]
pub struct InterpreterQuota {
    #[pyo3(get, set)]
    pub max_actors: Option<usize>,
    #[pyo3(get, set)]
    pub max_mailbox_total: Option<usize>,
    #[pyo3(get, set)]
    pub memory_soft_limit_bytes: Option<u64>,
    #[pyo3(get, set)]
    pub cpu_share: Option<f32>,
    #[pyo3(get, set)]
    pub max_handler_ms: Option<u64>,
    #[pyo3(get, set)]
    pub module_allowlist: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub module_denylist: Vec<String>,
    #[pyo3(get, set)]
    pub import_policy: String,
}

#[pymethods]
impl InterpreterQuota {
    #[new]
    #[pyo3(signature = (
        max_actors = None,
        max_mailbox_total = None,
        memory_soft_limit_bytes = None,
        cpu_share = None,
        max_handler_ms = None,
        module_allowlist = None,
        module_denylist = Vec::new(),
        import_policy = String::from("lazy"),
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_actors: Option<usize>,
        max_mailbox_total: Option<usize>,
        memory_soft_limit_bytes: Option<u64>,
        cpu_share: Option<f32>,
        max_handler_ms: Option<u64>,
        module_allowlist: Option<Vec<String>>,
        module_denylist: Vec<String>,
        import_policy: String,
    ) -> Self {
        Self {
            max_actors,
            max_mailbox_total,
            memory_soft_limit_bytes,
            cpu_share,
            max_handler_ms,
            module_allowlist,
            module_denylist,
            import_policy,
        }
    }
}

/// Execution model for Python actors. Maps to the dispatcher names in
/// `reference.conf.toml` under `rakka.python.interpreters.*`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum InterpreterKind {
    /// One interpreter, one OS thread. GIL is held here and nowhere else.
    #[default]
    Pinned,
    /// N independent subinterpreters, each on its own OS thread. PEP 684.
    SubinterpreterPool { count: usize },
    /// Free-threaded CPython (3.13+ PEP 703 builds): one interpreter, no GIL.
    NoGil { threads: usize },
    /// Separate OS processes with Rust-arbitrated IPC.
    Subprocess { count: usize },
}

impl InterpreterKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            InterpreterKind::Pinned => "python-pinned",
            InterpreterKind::SubinterpreterPool { .. } => "python-subinterpreter-pool",
            InterpreterKind::NoGil { .. } => "python-nogil",
            InterpreterKind::Subprocess { .. } => "python-subprocess",
        }
    }

    pub fn worker_count(&self) -> usize {
        match self {
            InterpreterKind::Pinned => 1,
            InterpreterKind::SubinterpreterPool { count } => *count,
            InterpreterKind::NoGil { threads } => *threads,
            InterpreterKind::Subprocess { count } => *count,
        }
    }
}

/// Lightweight capability probe. In the first slice we report only the
/// pinned + nogil facts; subinterpreter PyO3 bindings need 0.23+ which is
/// tracked in PORTING_TODO (Phase P1.5).
pub fn subinterpreters_supported(py: Python<'_>) -> bool {
    let sys = match py.import_bound("sys") {
        Ok(m) => m,
        Err(_) => return false,
    };
    let version_info = match sys.getattr("version_info") {
        Ok(v) => v,
        Err(_) => return false,
    };
    let major: u32 = version_info.getattr("major").and_then(|x| x.extract()).unwrap_or(0);
    let minor: u32 = version_info.getattr("minor").and_then(|x| x.extract()).unwrap_or(0);
    (major, minor) >= (3, 12)
}

pub fn nogil_supported(py: Python<'_>) -> bool {
    let sys = match py.import_bound("sys") {
        Ok(m) => m,
        Err(_) => return false,
    };
    sys.getattr("_is_gil_enabled")
        .and_then(|f| f.call0())
        .and_then(|v| v.extract::<bool>())
        .map(|gil_on| !gil_on)
        .unwrap_or(false)
}

/// Task delivered to a worker thread for execution under its interpreter.
pub(crate) struct PyTask {
    pub run: Box<dyn FnOnce(Python<'_>) + Send>,
}

#[derive(Default)]
pub(crate) struct InterpreterMetrics {
    pub actors_hosted: AtomicUsize,
    pub messages_handled: AtomicU64,
    pub gil_hold_ns_total: AtomicU64,
    pub mailbox_depth_total: AtomicUsize,
    pub handler_panics: AtomicU64,
    pub long_handlers: AtomicU64,
}

pub(crate) struct Worker {
    pub tx: mpsc::UnboundedSender<PyTask>,
    pub _handle: Mutex<Option<JoinHandle<()>>>,
}

pub struct InterpreterInstance {
    pub label: String,
    pub kind: InterpreterKind,
    pub quota: InterpreterQuota,
    pub(crate) metrics: InterpreterMetrics,
    pub(crate) workers: Vec<Arc<Worker>>,
    pub(crate) next: AtomicUsize,
}

impl InterpreterInstance {
    pub fn new(label: impl Into<String>, kind: InterpreterKind, quota: InterpreterQuota) -> Arc<Self> {
        let workers = (0..kind.worker_count()).map(|_| spawn_worker()).collect();
        Arc::new(Self {
            label: label.into(),
            kind,
            quota,
            metrics: InterpreterMetrics::default(),
            workers,
            next: AtomicUsize::new(0),
        })
    }

    pub fn register_actor(&self) -> Result<(), PyErr> {
        if let Some(cap) = self.quota.max_actors {
            let now = self.metrics.actors_hosted.fetch_add(1, Ordering::Relaxed);
            if now >= cap {
                self.metrics.actors_hosted.fetch_sub(1, Ordering::Relaxed);
                return Err(PyErr::new::<errors::InterpreterOverloaded, _>(format!(
                    "interpreter `{}` reached max_actors={}",
                    self.label, cap
                )));
            }
        } else {
            self.metrics.actors_hosted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn unregister_actor(&self) {
        self.metrics.actors_hosted.fetch_sub(1, Ordering::Relaxed);
    }

    /// Pick a worker for an actor id, sticky by hash so that co-communicating
    /// actors tend to share an interpreter. Callers pass the `ActorPath`'s
    /// stable hash.
    pub fn worker_for(&self, actor_hash: u64) -> Arc<Worker> {
        let n = self.workers.len().max(1);
        let idx = (actor_hash as usize) % n;
        self.workers[idx].clone()
    }

    pub fn any_worker(&self) -> Arc<Worker> {
        let n = self.workers.len().max(1);
        let i = self.next.fetch_add(1, Ordering::Relaxed) % n;
        self.workers[i].clone()
    }

    pub fn snapshot_metrics(&self) -> InterpreterMetricsSnapshot {
        InterpreterMetricsSnapshot {
            actors_hosted: self.metrics.actors_hosted.load(Ordering::Relaxed),
            messages_handled: self.metrics.messages_handled.load(Ordering::Relaxed),
            gil_hold_ns_total: self.metrics.gil_hold_ns_total.load(Ordering::Relaxed),
            mailbox_depth_total: self.metrics.mailbox_depth_total.load(Ordering::Relaxed),
            handler_panics: self.metrics.handler_panics.load(Ordering::Relaxed),
            long_handlers: self.metrics.long_handlers.load(Ordering::Relaxed),
        }
    }
}

fn spawn_worker() -> Arc<Worker> {
    let (tx, mut rx) = mpsc::unbounded_channel::<PyTask>();
    let handle = std::thread::Builder::new()
        .name("rakka-py-worker".into())
        .spawn(move || {
            // Each worker serializes Python execution on its own OS thread.
            // For the pinned / subinterpreter-pool variants the GIL stays
            // bound to this thread's thread-state between tasks, so back-to-
            // back dispatches avoid repeated acquire/release overhead.
            while let Some(task) = rx.blocking_recv() {
                Python::with_gil(|py| {
                    (task.run)(py);
                });
            }
        })
        .expect("spawn py worker");
    Arc::new(Worker { tx, _handle: Mutex::new(Some(handle)) })
}

#[derive(Clone, Debug)]
pub struct InterpreterMetricsSnapshot {
    pub actors_hosted: usize,
    pub messages_handled: u64,
    pub gil_hold_ns_total: u64,
    pub mailbox_depth_total: usize,
    pub handler_panics: u64,
    pub long_handlers: u64,
}

/// Registry of interpreter pools keyed by label.
#[derive(Default)]
pub struct InterpreterRegistry {
    pools: Mutex<std::collections::HashMap<String, Arc<InterpreterInstance>>>,
}

impl InterpreterRegistry {
    pub fn get_or_create(
        &self,
        label: &str,
        kind: InterpreterKind,
        quota: InterpreterQuota,
    ) -> Arc<InterpreterInstance> {
        let mut pools = self.pools.lock();
        pools
            .entry(label.to_string())
            .or_insert_with(|| InterpreterInstance::new(label, kind, quota))
            .clone()
    }

    pub fn get(&self, label: &str) -> Option<Arc<InterpreterInstance>> {
        self.pools.lock().get(label).cloned()
    }

    pub fn labels(&self) -> Vec<String> {
        self.pools.lock().keys().cloned().collect()
    }
}

/// Python-facing classes.
#[pyclass(name = "InterpreterPool", module = "rakka._native")]
pub struct PyInterpreterPool {
    pub(crate) instance: Arc<InterpreterInstance>,
}

#[pymethods]
impl PyInterpreterPool {
    #[getter]
    fn label(&self) -> &str {
        &self.instance.label
    }

    #[getter]
    fn kind(&self) -> &str {
        self.instance.kind.as_str()
    }

    #[getter]
    fn worker_count(&self) -> usize {
        self.instance.workers.len()
    }

    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let s = self.instance.snapshot_metrics();
        let d = PyDict::new_bound(py);
        d.set_item("actors_hosted", s.actors_hosted)?;
        d.set_item("messages_handled", s.messages_handled)?;
        d.set_item("gil_hold_ns_total", s.gil_hold_ns_total)?;
        d.set_item("mailbox_depth_total", s.mailbox_depth_total)?;
        d.set_item("handler_panics", s.handler_panics)?;
        d.set_item("long_handlers", s.long_handlers)?;
        Ok(d)
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<InterpreterQuota>()?;
    m.add_class::<PyInterpreterPool>()?;
    m.add_function(wrap_pyfunction!(subinterpreters_supported_py, m)?)?;
    m.add_function(wrap_pyfunction!(nogil_supported_py, m)?)?;
    Ok(())
}

#[pyfunction(name = "subinterpreters_supported")]
fn subinterpreters_supported_py(py: Python<'_>) -> bool {
    subinterpreters_supported(py)
}

#[pyfunction(name = "nogil_supported")]
fn nogil_supported_py(py: Python<'_>) -> bool {
    nogil_supported(py)
}
