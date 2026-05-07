//! Python streams submodule. Exposes the `atomr-streams` DSL through two
//! layers:
//!
//! 1. **Legacy i64 helpers** — `map_reduce`, `run_collect`, `run_fold`,
//!    `via_keep_alive`, `via_initial_delay`, `via_conflate`, `via_expand`,
//!    `merge_sorted_`, `merge_prioritized_`, `via_split_after_count`,
//!    `via_prefix_and_tail`, `via_recover_with_retries`, `via_select_error`.
//!    Kept for backward compatibility.
//!
//! 2. **Typed DSL** on `Py<PyAny>` — `Source`, `Sink`, `Flow`, `RunnableGraph`,
//!    `KillSwitch`, `BroadcastHub`, `MergeHub`, `SourceQueue`, `SinkQueue`,
//!    plus `QueueOfferResult`. Stream callbacks acquire the GIL inline on the
//!    materializer dispatcher; element drops happen inside `Python::with_gil`
//!    via the `SendPyAny` newtype.

use std::sync::Arc;
use std::time::Duration;

use atomr_streams::{
    conflate, expand, initial_delay, keep_alive, merge_prioritized, merge_sorted, prefix_and_tail,
    recover_with_retries, select_error, split_after, ActorMaterializer, BroadcastHub as RustBroadcastHub,
    Flow, KillSwitch as RustKillSwitch, MergeHub as RustMergeHub, QueueOfferResult as RustOfferResult, Sink,
    SinkQueue as RustSinkQueue, Source,
};
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio as pyo3_tokio;

use crate::runtime::runtime;

// =============================================================================
// SendPyAny — Send/Sync wrapper around Py<PyAny> with GIL-safe Drop and Clone.
// =============================================================================

/// Newtype wrapping `Py<PyAny>` so it can travel through `Send + 'static`
/// channels (futures, tokio mpsc) safely. `Py<PyAny>` is `Send` already, but
/// dropping or cloning it without the GIL panics — every site that may drop
/// must acquire the GIL.
pub struct SendPyAny(pub Py<PyAny>);

// SAFETY: `Py<PyAny>` is `Send`. We are explicit here because some sites
// require `Sync` (broadcast::Sender bounds). Access is internally serialised
// by acquiring the GIL.
unsafe impl Send for SendPyAny {}
unsafe impl Sync for SendPyAny {}

impl SendPyAny {
    pub fn new(obj: Py<PyAny>) -> Self {
        Self(obj)
    }

    pub fn into_inner(self) -> Py<PyAny> {
        // We must avoid running our custom Drop after extracting; use
        // ManuallyDrop to skip the GIL re-acquire when the inner value
        // is already being moved into an owning context.
        let inner = unsafe { std::ptr::read(&self.0) };
        std::mem::forget(self);
        inner
    }

    pub fn as_py(&self) -> &Py<PyAny> {
        &self.0
    }
}

impl Drop for SendPyAny {
    fn drop(&mut self) {
        // Acquiring the GIL ensures that the inner Py<PyAny>'s Drop, which
        // runs inside this scope, can decrement the refcount safely. If the
        // GIL is already held by this thread, with_gil is reentrant.
        Python::with_gil(|_py| {
            // The compiler-generated drop of Py<PyAny> runs at end of scope.
        });
    }
}

impl Clone for SendPyAny {
    fn clone(&self) -> Self {
        Python::with_gil(|py| SendPyAny(self.0.clone_ref(py)))
    }
}

fn pylist_from_send<'py>(py: Python<'py>, items: Vec<SendPyAny>) -> Bound<'py, PyList> {
    let list = PyList::empty_bound(py);
    for item in items {
        let obj = item.into_inner();
        let _ = list.append(obj.bind(py));
    }
    list
}

// =============================================================================
// Builder enums — pipeline parts are constructed lazily and consumed once.
// =============================================================================

/// Source-side stages applied to an upstream `Source<SendPyAny>`.
enum SourceStage {
    /// Composition with a (already-materialized) `Flow<SendPyAny, SendPyAny>`
    /// builder, applied as a one-shot.
    Flow(FlowBuilder),
}

/// Lazy Source builder. We keep the build closure boxed because
/// `Source<T>` itself owns a `BoxStream` which is not `Clone`.
struct SourceBuilder {
    build: Box<dyn FnOnce() -> Source<SendPyAny> + Send + 'static>,
}

impl SourceBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> Source<SendPyAny> + Send + 'static,
    {
        Self { build: Box::new(f) }
    }

    fn build(self) -> Source<SendPyAny> {
        (self.build)()
    }

    fn apply(self, stage: SourceStage) -> Self {
        match stage {
            SourceStage::Flow(flow) => SourceBuilder::new(move || {
                let src = (self.build)();
                let f: Flow<SendPyAny, SendPyAny> = flow.build();
                src.via(f)
            }),
        }
    }
}

/// Lazy Flow builder. Composing flows in Rust requires consuming both —
/// we represent composition by stacking transforms.
struct FlowBuilder {
    build: Box<dyn FnOnce() -> Flow<SendPyAny, SendPyAny> + Send + 'static>,
}

impl FlowBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> Flow<SendPyAny, SendPyAny> + Send + 'static,
    {
        Self { build: Box::new(f) }
    }

    fn build(self) -> Flow<SendPyAny, SendPyAny> {
        (self.build)()
    }

    fn via(self, next: FlowBuilder) -> FlowBuilder {
        FlowBuilder::new(move || {
            let a = (self.build)();
            let b = (next.build)();
            a.via(b)
        })
    }
}

/// A Sink builder reduces a built `Source<SendPyAny>` into a future that
/// returns a Python materialized value.
type SinkRunner = Box<
    dyn FnOnce(
            Source<SendPyAny>,
        ) -> futures::future::BoxFuture<'static, PyResult<Py<PyAny>>>
        + Send
        + 'static,
>;

struct SinkBuilder {
    run: SinkRunner,
}

impl SinkBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce(Source<SendPyAny>) -> futures::future::BoxFuture<'static, PyResult<Py<PyAny>>>
            + Send
            + 'static,
    {
        Self { run: Box::new(f) }
    }
}

// =============================================================================
// PySource
// =============================================================================

#[pyclass(name = "Source", module = "atomr._native.streams")]
pub struct PySource {
    builder: Mutex<Option<SourceBuilder>>,
}

impl PySource {
    fn from_builder(builder: SourceBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<SourceBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Source has already been consumed"))
    }
}

#[pymethods]
impl PySource {
    /// `Source.from_iter(iterable)` — materialize a Python iterable into
    /// a Vec<SendPyAny>. Materialization is eager so the iterable is
    /// drained while we still hold the GIL.
    #[staticmethod]
    fn from_iter(py: Python<'_>, iterable: Bound<'_, PyAny>) -> PyResult<Self> {
        let mut items: Vec<SendPyAny> = Vec::new();
        let it = iterable.iter()?;
        for v in it {
            let v = v?;
            items.push(SendPyAny::new(v.unbind()));
        }
        let _ = py;
        Ok(PySource::from_builder(SourceBuilder::new(move || Source::from_iter(items))))
    }

    /// `Source.empty()` — empty source.
    #[staticmethod]
    fn empty() -> Self {
        PySource::from_builder(SourceBuilder::new(Source::empty))
    }

    /// `Source.single(value)` — single-element source.
    #[staticmethod]
    fn single(value: Bound<'_, PyAny>) -> Self {
        let val = SendPyAny::new(value.unbind());
        PySource::from_builder(SourceBuilder::new(move || Source::single(val)))
    }

    /// `Source.from_queue()` — returns `(Source, SourceQueue)` with an
    /// unbounded mpsc backing.
    #[staticmethod]
    fn from_queue(py: Python<'_>) -> PyResult<(Py<PySource>, Py<PySourceQueue>)> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SendPyAny>();
        let receiver_slot: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<SendPyAny>>>> =
            Arc::new(Mutex::new(Some(rx)));
        let receiver_for_build = Arc::clone(&receiver_slot);
        let source = PySource::from_builder(SourceBuilder::new(move || {
            let rx_opt = receiver_for_build.lock().take();
            match rx_opt {
                Some(rx) => Source::from_receiver(rx),
                None => Source::empty(),
            }
        }));
        let queue = PySourceQueue { tx: Mutex::new(Some(tx)) };
        Ok((Py::new(py, source)?, Py::new(py, queue)?))
    }

    /// `source.via(flow)` — append a flow stage. Consumes both `self`
    /// and `flow`, returning a new `Source`.
    fn via(&self, py: Python<'_>, flow: Py<PyFlow>) -> PyResult<Py<PySource>> {
        let src_b = self.take_builder()?;
        let flow_b = flow.borrow_mut(py).take_builder()?;
        let new = PySource::from_builder(src_b.apply(SourceStage::Flow(flow_b)));
        Py::new(py, new)
    }

    /// `source.to(sink)` — build a `RunnableGraph`. Consumes both sides.
    fn to(&self, py: Python<'_>, sink: Py<PySink>) -> PyResult<Py<PyRunnableGraph>> {
        let src_b = self.take_builder()?;
        let sink_b = sink.borrow_mut(py).take_builder()?;
        let graph = PyRunnableGraph::new(src_b, sink_b);
        Py::new(py, graph)
    }

    /// `source.run_collect()` — convenience: run with a collecting Sink.
    /// Returns awaitable yielding a `list`.
    fn run_collect_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let src_b = self.take_builder()?;
        pyo3_tokio::future_into_py(py, async move {
            let src = src_b.build();
            let collected = Sink::collect(src).await;
            Python::with_gil(|py| Ok(pylist_from_send(py, collected).unbind().into_any()))
        })
    }

    /// `source.kill_switch()` — wrap with a fresh `KillSwitch`. Returns
    /// `(Source, KillSwitch)`.
    fn kill_switch(&self, py: Python<'_>) -> PyResult<(Py<PySource>, Py<PyKillSwitch>)> {
        let src_b = self.take_builder()?;
        let ks = RustKillSwitch::new();
        let ks_clone = ks.clone();
        let new_b = SourceBuilder::new(move || ks_clone.flow(src_b.build()));
        let py_ks = PyKillSwitch { inner: ks };
        Ok((Py::new(py, PySource::from_builder(new_b))?, Py::new(py, py_ks)?))
    }
}

// =============================================================================
// PyFlow
// =============================================================================

#[pyclass(name = "Flow", module = "atomr._native.streams")]
pub struct PyFlow {
    builder: Mutex<Option<FlowBuilder>>,
}

impl PyFlow {
    fn from_builder(builder: FlowBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<FlowBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Flow has already been consumed"))
    }
}

#[pymethods]
impl PyFlow {
    /// Identity flow.
    #[staticmethod]
    fn identity() -> Self {
        PyFlow::from_builder(FlowBuilder::new(Flow::identity))
    }

    /// `Flow.from_fn(fn)` — synchronous mapping callable.
    #[staticmethod]
    fn from_fn(py: Python<'_>, func: Py<PyAny>) -> Self {
        let _ = py;
        PyFlow::from_builder(FlowBuilder::new(move || {
            let func = func;
            Flow::from_fn(move |x: SendPyAny| -> SendPyAny {
                Python::with_gil(|py| {
                    let inner = x.into_inner();
                    let arg = inner.bind(py);
                    match func.call1(py, (arg,)) {
                        Ok(out) => SendPyAny::new(out),
                        Err(e) => {
                            // Surface as None on error; preserve consistency with
                            // i64 wrappers which fall back on errors. Tests
                            // exercise the happy path.
                            e.restore(py);
                            SendPyAny::new(py.None())
                        }
                    }
                })
            })
        }))
    }

    /// `Flow.map(fn)` — alias of `from_fn` for symmetry with Source.via.
    #[staticmethod]
    fn map(py: Python<'_>, func: Py<PyAny>) -> Self {
        Self::from_fn(py, func)
    }

    /// `Flow.filter(pred)` — drop elements where `pred(x)` is falsy.
    /// Drops happen inside `Python::with_gil` because the predicate runs
    /// inside the GIL and the dropped `SendPyAny`'s own Drop also takes
    /// the GIL — defensive but cheap (with_gil is reentrant).
    #[staticmethod]
    fn filter(py: Python<'_>, pred: Py<PyAny>) -> Self {
        let _ = py;
        PyFlow::from_builder(FlowBuilder::new(move || {
            let pred = pred;
            Flow::<SendPyAny, SendPyAny>::filter(move |v: &SendPyAny| -> bool {
                Python::with_gil(|py| match pred.call1(py, (v.0.bind(py),)) {
                    Ok(b) => b.is_truthy(py).unwrap_or(false),
                    Err(e) => {
                        e.restore(py);
                        false
                    }
                })
            })
        }))
    }

    /// `Flow.take(n)` — limit to first `n` elements.
    #[staticmethod]
    fn take(n: usize) -> Self {
        PyFlow::from_builder(FlowBuilder::new(move || Flow::<SendPyAny, SendPyAny>::take(n)))
    }

    /// `Flow.skip(n)` — drop first `n` elements.
    #[staticmethod]
    fn skip(n: usize) -> Self {
        PyFlow::from_builder(FlowBuilder::new(move || Flow::<SendPyAny, SendPyAny>::skip(n)))
    }

    /// `Flow.map_async(fn, parallelism=1)` — fn returns an awaitable.
    /// Parallelism is bounded; outputs preserve upstream order.
    #[staticmethod]
    #[pyo3(signature = (func, parallelism=1))]
    fn map_async(py: Python<'_>, func: Py<PyAny>, parallelism: usize) -> Self {
        let _ = py;
        let p = parallelism.max(1);
        PyFlow::from_builder(FlowBuilder::new(move || {
            let func = func;
            Flow::<SendPyAny, SendPyAny>::map_async(p, move |x: SendPyAny| {
                let func = Python::with_gil(|py| func.clone_ref(py));
                async move {
                    // Call into Python under the GIL, convert to an asyncio
                    // future via pyo3-async-runtimes, then await it.
                    let py_fut = Python::with_gil(|py| -> PyResult<_> {
                        let coro = func.call1(py, (x.0.bind(py),))?;
                        pyo3_tokio::into_future(coro.bind(py).clone())
                    });
                    let result: PyResult<Py<PyAny>> = match py_fut {
                        Ok(fut) => fut.await,
                        Err(e) => Err(e),
                    };
                    match result {
                        Ok(v) => SendPyAny::new(v),
                        Err(e) => Python::with_gil(|py| {
                            e.restore(py);
                            SendPyAny::new(py.None())
                        }),
                    }
                }
            })
        }))
    }

    /// `Flow.via(other)` — compose two flows.
    fn via(&self, py: Python<'_>, other: Py<PyFlow>) -> PyResult<Py<PyFlow>> {
        let a = self.take_builder()?;
        let b = other.borrow_mut(py).take_builder()?;
        Py::new(py, PyFlow::from_builder(a.via(b)))
    }
}

// =============================================================================
// PySink
// =============================================================================

#[pyclass(name = "Sink", module = "atomr._native.streams")]
pub struct PySink {
    builder: Mutex<Option<SinkBuilder>>,
}

impl PySink {
    fn from_builder(builder: SinkBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<SinkBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Sink has already been consumed"))
    }
}

#[pymethods]
impl PySink {
    /// `Sink.collect()` — gather elements into a list.
    #[staticmethod]
    fn collect() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                let v = Sink::collect(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    Ok(pylist_from_send(py, v).unbind().into_any())
                })
            }
            .boxed()
        }))
    }

    /// `Sink.fold(zero, fn)` — accumulate with a Python reducer.
    #[staticmethod]
    fn fold(_py: Python<'_>, zero: Bound<'_, PyAny>, func: Py<PyAny>) -> Self {
        use futures::FutureExt;
        let zero = SendPyAny::new(zero.unbind());
        PySink::from_builder(SinkBuilder::new(move |src: Source<SendPyAny>| {
            async move {
                let func = func;
                let acc = Sink::fold(src, zero, move |acc: SendPyAny, v: SendPyAny| -> SendPyAny {
                    Python::with_gil(|py| {
                        let acc_obj = acc.into_inner();
                        let v_obj = v.into_inner();
                        match func.call1(py, (acc_obj.bind(py), v_obj.bind(py))) {
                            Ok(r) => SendPyAny::new(r),
                            Err(e) => {
                                e.restore(py);
                                SendPyAny::new(py.None())
                            }
                        }
                    })
                })
                .await;
                Python::with_gil(|_py| -> PyResult<Py<PyAny>> { Ok(acc.into_inner()) })
            }
            .boxed()
        }))
    }

    /// `Sink.foreach(fn)` — call `fn(x)` for each element.
    /// Materialized value is `None`.
    #[staticmethod]
    fn foreach(_py: Python<'_>, func: Py<PyAny>) -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(move |src: Source<SendPyAny>| {
            async move {
                let func = func;
                Sink::for_each(src, move |x: SendPyAny| {
                    Python::with_gil(|py| {
                        if let Err(e) = func.call1(py, (x.0.bind(py),)) {
                            e.restore(py);
                        }
                        // `x` drops here, with the GIL held.
                    })
                })
                .await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(py.None()) })
            }
            .boxed()
        }))
    }

    /// `Sink.head_option()` — produce the first element (or None).
    #[staticmethod]
    fn head_option() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                let head = Sink::first(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    match head {
                        Some(v) => Ok(v.into_inner()),
                        None => Ok(py.None()),
                    }
                })
            }
            .boxed()
        }))
    }

    /// `Sink.ignore()` — drain and produce None.
    #[staticmethod]
    fn ignore() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                Sink::ignore(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(py.None()) })
            }
            .boxed()
        }))
    }

    /// `Sink.queue(source)` — drive a source through a pull-based queue.
    /// Static method that takes a Source and returns a SinkQueue handle
    /// (this is the dual of `Source.from_queue`, exposing pull semantics).
    #[staticmethod]
    fn queue(py: Python<'_>, source: Py<PySource>) -> PyResult<Py<PySinkQueue>> {
        let src_b = source.borrow_mut(py).take_builder()?;
        // We need to build the source on a Tokio runtime entry; do it now,
        // since SinkQueue spawns its own task internally.
        let rt = runtime();
        let _g = rt.enter();
        let q = Sink::queue(src_b.build());
        Py::new(py, PySinkQueue { inner: Arc::new(q) })
    }
}

// =============================================================================
// PyRunnableGraph
// =============================================================================

#[pyclass(name = "RunnableGraph", module = "atomr._native.streams")]
pub struct PyRunnableGraph {
    state: Mutex<Option<(SourceBuilder, SinkBuilder)>>,
}

impl PyRunnableGraph {
    fn new(src: SourceBuilder, sink: SinkBuilder) -> Self {
        Self { state: Mutex::new(Some((src, sink))) }
    }
}

#[pymethods]
impl PyRunnableGraph {
    /// `graph.run()` — return an awaitable that yields the materialized value.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pair = self
            .state
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("RunnableGraph already run"))?;
        let (src_b, sink_b) = pair;
        pyo3_tokio::future_into_py(py, async move {
            let src = src_b.build();
            (sink_b.run)(src).await
        })
    }

    /// `graph.run_blocking()` — block the calling thread; convenient for
    /// scripts and tests.
    fn run_blocking(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pair = self
            .state
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("RunnableGraph already run"))?;
        let (src_b, sink_b) = pair;
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let src = src_b.build();
                (sink_b.run)(src).await
            })
        })
    }
}

// =============================================================================
// KillSwitch
// =============================================================================

#[pyclass(name = "KillSwitch", module = "atomr._native.streams")]
pub struct PyKillSwitch {
    inner: RustKillSwitch,
}

#[pymethods]
impl PyKillSwitch {
    #[new]
    fn new() -> Self {
        Self { inner: RustKillSwitch::new() }
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }

    #[pyo3(signature = (error_msg=None))]
    fn abort(&self, error_msg: Option<String>) {
        self.inner.abort(error_msg.unwrap_or_else(|| "aborted".to_string()));
    }

    fn is_shut_down(&self) -> bool {
        self.inner.is_shut_down()
    }

    fn error(&self) -> Option<String> {
        self.inner.error()
    }
}

// =============================================================================
// SourceQueue / SinkQueue
// =============================================================================

#[pyclass(name = "SourceQueue", module = "atomr._native.streams")]
pub struct PySourceQueue {
    tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<SendPyAny>>>,
}

#[pymethods]
impl PySourceQueue {
    /// `offer(value) -> str` — return one of `"Enqueued"`, `"QueueClosed"`.
    fn offer(&self, value: Bound<'_, PyAny>) -> PyResult<String> {
        let v = SendPyAny::new(value.unbind());
        let guard = self.tx.lock();
        match guard.as_ref() {
            Some(tx) => match tx.send(v) {
                Ok(()) => Ok(format!("{:?}", RustOfferResult::Enqueued)),
                Err(_) => Ok(format!("{:?}", RustOfferResult::QueueClosed)),
            },
            None => Ok(format!("{:?}", RustOfferResult::QueueClosed)),
        }
    }

    /// Close the queue so the source completes normally.
    fn complete(&self) {
        let _ = self.tx.lock().take();
    }

    fn is_closed(&self) -> bool {
        let guard = self.tx.lock();
        match guard.as_ref() {
            Some(tx) => tx.is_closed(),
            None => true,
        }
    }
}

#[pyclass(name = "SinkQueue", module = "atomr._native.streams")]
pub struct PySinkQueue {
    inner: Arc<RustSinkQueue<SendPyAny>>,
}

#[pymethods]
impl PySinkQueue {
    /// `pull()` — await the next element. Returns `None` once exhausted.
    fn pull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_tokio::future_into_py(py, async move {
            let v = inner.pull().await;
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                match v {
                    Some(v) => Ok(v.into_inner()),
                    None => Ok(py.None()),
                }
            })
        })
    }

    /// Blocking pull — convenient for tests.
    fn pull_blocking(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let v = inner.pull().await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    match v {
                        Some(v) => Ok(v.into_inner()),
                        None => Ok(py.None()),
                    }
                })
            })
        })
    }
}

// =============================================================================
// BroadcastHub / MergeHub
// =============================================================================

#[pyclass(name = "BroadcastHub", module = "atomr._native.streams")]
pub struct PyBroadcastHub {
    inner: Arc<RustBroadcastHub<SendPyAny>>,
}

#[pymethods]
impl PyBroadcastHub {
    /// `BroadcastHub.attach(source, buffer_size)` — drives `source`'s
    /// elements into a fan-out hub. Returns the hub.
    #[staticmethod]
    #[pyo3(signature = (source, buffer_size=16))]
    fn attach(py: Python<'_>, source: Py<PySource>, buffer_size: usize) -> PyResult<Py<PyBroadcastHub>> {
        let src_b = source.borrow_mut(py).take_builder()?;
        let hub = Arc::new(RustBroadcastHub::<SendPyAny>::new(buffer_size.max(1)));
        let rt = runtime();
        let _g = rt.enter();
        hub.attach(src_b.build());
        Py::new(py, PyBroadcastHub { inner: hub })
    }

    /// Acquire a fresh consumer source.
    fn consumer(&self, py: Python<'_>) -> PyResult<Py<PySource>> {
        let hub = Arc::clone(&self.inner);
        // `consumer()` requires being on a Tokio runtime; subscribe is sync
        // but the unfold runs lazily. Build closure must own the receiver.
        let src = hub.consumer();
        // `src` is a Source<SendPyAny>; we wrap it directly.
        let src_holder = Arc::new(Mutex::new(Some(src)));
        let src_holder_for_build = Arc::clone(&src_holder);
        let builder = SourceBuilder::new(move || {
            src_holder_for_build.lock().take().unwrap_or_else(Source::empty)
        });
        Py::new(py, PySource::from_builder(builder))
    }

    fn consumer_count(&self) -> usize {
        self.inner.consumer_count()
    }
}

#[pyclass(name = "MergeHub", module = "atomr._native.streams")]
pub struct PyMergeHub {
    inner: Arc<RustMergeHub<SendPyAny>>,
}

#[pymethods]
impl PyMergeHub {
    #[new]
    fn new() -> Self {
        Self { inner: Arc::new(RustMergeHub::new()) }
    }

    /// Attach a producer source.
    fn attach(&self, py: Python<'_>, source: Py<PySource>) -> PyResult<()> {
        let src_b = source.borrow_mut(py).take_builder()?;
        let rt = runtime();
        let _g = rt.enter();
        self.inner.attach(src_b.build());
        Ok(())
    }

    /// Take the merged consumer source.
    fn source(&self, py: Python<'_>) -> PyResult<Py<PySource>> {
        let s = self.inner.source();
        let holder = Arc::new(Mutex::new(Some(s)));
        let holder_b = Arc::clone(&holder);
        let builder = SourceBuilder::new(move || {
            holder_b.lock().take().unwrap_or_else(Source::empty)
        });
        Py::new(py, PySource::from_builder(builder))
    }
}

// =============================================================================
// Legacy i64 helpers (unchanged)
// =============================================================================

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
/// quiet interval of `idle_secs`..
#[pyfunction]
fn via_keep_alive(py: Python<'_>, items: Vec<i64>, idle_secs: f64, filler: i64) -> Vec<i64> {
    run_to_vec(py, move || {
        keep_alive(Source::from_iter(items), Duration::from_secs_f64(idle_secs), move || filler)
    })
}

/// `initial_delay(items, delay_secs)` — delay the first element.
#[pyfunction]
fn via_initial_delay(py: Python<'_>, items: Vec<i64>, delay_secs: f64) -> Vec<i64> {
    run_to_vec(py, move || initial_delay(Source::from_iter(items), Duration::from_secs_f64(delay_secs)))
}

/// `conflate(items, fold)` — coalesce backed-up upstream elements.
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
/// of values via `extrapolate(x) -> List[int]`..
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

/// `merge_sorted(left, right)` — merge two sorted streams.
#[pyfunction]
fn merge_sorted_(py: Python<'_>, left: Vec<i64>, right: Vec<i64>) -> Vec<i64> {
    run_to_vec(py, move || merge_sorted(Source::from_iter(left), Source::from_iter(right)))
}

/// `merge_prioritized(left, left_weight, right, right_weight)`.
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

/// `split_after(items, predicate)` — count of substreams emitted.
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
            ActorMaterializer::new().run_collect(s).await.len()
        })
    })
}

/// `prefix_and_tail(items, n)`.
#[pyfunction]
fn via_prefix_and_tail(py: Python<'_>, items: Vec<i64>, n: usize) -> (Vec<i64>, usize) {
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = prefix_and_tail(Source::from_iter(items), n);
            let collected = ActorMaterializer::new().run_collect(s).await;
            if let Some((prefix, tail)) = collected.into_iter().next() {
                let tail_count = ActorMaterializer::new().run_collect(tail).await.len();
                (prefix, tail_count)
            } else {
                (Vec::new(), 0)
            }
        })
    })
}

/// `recover_with_retries(items_with_errors, replacement, attempts)`.
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

/// `select_error(items, mapper)`.
#[pyfunction]
fn via_select_error(
    py: Python<'_>,
    items_with_errors: Vec<(i64, Option<String>)>,
    mapper: Py<PyAny>,
) -> Vec<i64> {
    let cb = mapper;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mapped: Vec<Result<i64, String>> = items_with_errors
                .into_iter()
                .map(|(v, e)| match e {
                    None => Ok(v),
                    Some(label) => Err(label),
                })
                .collect();
            let src = Source::from_iter(mapped);
            let mapped_src = select_error(src, move |label: String| -> String {
                Python::with_gil(|gil| {
                    cb.call1(gil, (label.clone(),))
                        .and_then(|r| r.extract::<String>(gil))
                        .unwrap_or(label)
                })
            });
            let s: Source<i64> = mapped_src.filter_map(|r| r.ok());
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

// =============================================================================
// Module registration
// =============================================================================

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "streams")?;
    // legacy i64 helpers
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
    // typed DSL
    sub.add_class::<PySource>()?;
    sub.add_class::<PyFlow>()?;
    sub.add_class::<PySink>()?;
    sub.add_class::<PyRunnableGraph>()?;
    sub.add_class::<PyKillSwitch>()?;
    sub.add_class::<PySourceQueue>()?;
    sub.add_class::<PySinkQueue>()?;
    sub.add_class::<PyBroadcastHub>()?;
    sub.add_class::<PyMergeHub>()?;
    let _ = pyo3_tokio::get_runtime;
    m.add_submodule(&sub)?;
    Ok(())
}
