//! Persistence submodule.
//!
//! Phase 4 of the Python-bindings expansion plan.
//!
//! Exposes the building blocks for the Python `EventSourcedActor` base
//! class (defined in `python/atomr/persistence.py`):
//!
//! * `InMemoryJournal` — thin wrapper over [`atomr_persistence::InMemoryJournal`]
//!   with blocking write/replay/snapshot helpers (preserved unchanged
//!   from earlier slices).
//! * `InMemorySnapshotStore` — wraps [`atomr_persistence::InMemorySnapshotStore`]
//!   with blocking save/load/delete.
//! * `RecoveryPermitter` — wraps [`atomr_persistence::RecoveryPermitter`]
//!   so Python can configure max concurrent recoveries.
//! * `Effect` — small Rust pyclass enum returned from
//!   `command_handler`. Variants: `persist(event)`,
//!   `persist_all(events)`, `snapshot(every=None)`,
//!   `reply_message(value)`, `stop()`, `none()`. The reply-payload
//!   field is exposed as `effect.value` (Phase 4 originally named the
//!   constructor `reply` and the field `reply_value`, which collided
//!   with the staticmethod-vs-getter name space — see the field
//!   docstring below for the migration note).
//!
//! The actual recovery / persist orchestration runs entirely in the
//! Python `EventSourcedActor` base class — see `python/atomr/persistence.py`.
//! This keeps Python events flowing through the Phase-9 codec registry
//! (`PyActorSystem.codecs`) without forcing the Rust side to know about
//! Python types.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use atomr_persistence::{
    InMemoryJournal, InMemorySnapshotStore, Journal, PersistentRepr, RecoveryPermitter, SnapshotMetadata,
    SnapshotStore,
};

use crate::errors;
use crate::runtime::runtime;

// ---------------------------------------------------------------------
// InMemoryJournal — preserved from earlier slices, only renamed module.
// ---------------------------------------------------------------------

#[pyclass(name = "InMemoryJournal", module = "atomr._native.persistence")]
pub struct PyInMemoryJournal {
    inner: Arc<InMemoryJournal>,
}

#[pymethods]
impl PyInMemoryJournal {
    #[new]
    fn new() -> Self {
        Self { inner: InMemoryJournal::new() }
    }

    #[pyo3(signature = (pid, seq, payload, tags=Vec::new()))]
    fn write(
        &self,
        py: Python<'_>,
        pid: String,
        seq: u64,
        payload: Bound<'_, PyBytes>,
        tags: Vec<String>,
    ) -> PyResult<()> {
        let inner = self.inner.clone();
        let bytes = payload.as_bytes().to_vec();
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let repr = PersistentRepr {
                    persistence_id: pid,
                    sequence_nr: seq,
                    payload: bytes,
                    deleted: false,
                    manifest: "bytes".into(),
                    writer_uuid: "pybindings".into(),
                    tags,
                };
                inner.write_messages(vec![repr]).await.map_err(errors::map)
            })
        })
    }

    /// Lower-level write: caller supplies manifest so the Phase-9
    /// codec round-trip can be replayed faithfully.
    #[pyo3(signature = (pid, seq, payload, manifest, tags=Vec::new()))]
    fn write_event(
        &self,
        py: Python<'_>,
        pid: String,
        seq: u64,
        payload: Bound<'_, PyBytes>,
        manifest: String,
        tags: Vec<String>,
    ) -> PyResult<()> {
        let inner = self.inner.clone();
        let bytes = payload.as_bytes().to_vec();
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let repr = PersistentRepr {
                    persistence_id: pid,
                    sequence_nr: seq,
                    payload: bytes,
                    deleted: false,
                    manifest,
                    writer_uuid: "pybindings".into(),
                    tags,
                };
                inner.write_messages(vec![repr]).await.map_err(errors::map)
            })
        })
    }

    fn replay<'py>(&self, py: Python<'py>, pid: String) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let reprs = py.allow_threads(|| {
            rt.block_on(async move {
                inner.replay_messages(&pid, 1, u64::MAX, u64::MAX).await.map_err(errors::map)
            })
        })?;
        let list = PyList::empty_bound(py);
        for r in reprs {
            list.append(PyBytes::new_bound(py, &r.payload))?;
        }
        Ok(list)
    }

    /// Replay events with full metadata: returns
    /// `[(seq_nr, payload_bytes, manifest, tags), ...]` so the
    /// Python `EventSourcedActor` can route through the codec
    /// registry per-event.
    #[pyo3(signature = (pid, from_seq=1, to_seq=u64::MAX, max=u64::MAX))]
    fn replay_events<'py>(
        &self,
        py: Python<'py>,
        pid: String,
        from_seq: u64,
        to_seq: u64,
        max: u64,
    ) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let reprs = py.allow_threads(|| {
            rt.block_on(async move {
                inner.replay_messages(&pid, from_seq, to_seq, max).await.map_err(errors::map)
            })
        })?;
        let list = PyList::empty_bound(py);
        for r in reprs {
            let tup: Py<PyAny> =
                (r.sequence_nr, PyBytes::new_bound(py, &r.payload).unbind(), r.manifest, r.tags).into_py(py);
            list.append(tup)?;
        }
        Ok(list)
    }

    fn highest_sequence_nr(&self, py: Python<'_>, pid: String) -> PyResult<u64> {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move { inner.highest_sequence_nr(&pid, 0).await.map_err(errors::map) })
        })
    }

    /// Replay all events tagged with `tag` starting at `from_offset`.
    /// Returns a list of `(persistence_id, sequence_nr, payload, tags)`
    /// tuples..
    #[pyo3(signature = (tag, from_offset=0, max=u64::MAX))]
    fn events_by_tag<'py>(
        &self,
        py: Python<'py>,
        tag: String,
        from_offset: u64,
        max: u64,
    ) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let reprs = py.allow_threads(|| {
            rt.block_on(async move { inner.events_by_tag(&tag, from_offset, max).await.map_err(errors::map) })
        })?;
        let list = PyList::empty_bound(py);
        for r in reprs {
            let tup: Py<PyAny> =
                (r.persistence_id, r.sequence_nr, PyBytes::new_bound(py, &r.payload).unbind(), r.tags)
                    .into_py(py);
            list.append(tup)?;
        }
        Ok(list)
    }

    /// Return distinct persistence ids known to the journal.
    fn all_persistence_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let ids = py.allow_threads(|| {
            rt.block_on(async move { inner.all_persistence_ids().await.map_err(errors::map) })
        })?;
        let list = PyList::empty_bound(py);
        for id in ids {
            list.append(id)?;
        }
        Ok(list)
    }
}

// ---------------------------------------------------------------------
// InMemorySnapshotStore
// ---------------------------------------------------------------------

#[pyclass(name = "InMemorySnapshotStore", module = "atomr._native.persistence")]
pub struct PyInMemorySnapshotStore {
    inner: Arc<InMemorySnapshotStore>,
}

#[pymethods]
impl PyInMemorySnapshotStore {
    #[new]
    fn new() -> Self {
        Self { inner: InMemorySnapshotStore::new() }
    }

    /// Save `payload` as the snapshot for `(pid, seq)`. Synchronous —
    /// drives the underlying async store on the shared runtime.
    fn save(&self, py: Python<'_>, pid: String, seq: u64, payload: Bound<'_, PyBytes>) -> PyResult<()> {
        let inner = self.inner.clone();
        let bytes = payload.as_bytes().to_vec();
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                inner
                    .save(
                        SnapshotMetadata { persistence_id: pid, sequence_nr: seq, timestamp: now_ms() },
                        bytes,
                    )
                    .await;
            })
        });
        Ok(())
    }

    /// Load the latest snapshot for `pid`. Returns
    /// `(sequence_nr, payload_bytes)` or `None` if no snapshot is
    /// stored.
    fn load<'py>(&self, py: Python<'py>, pid: String) -> PyResult<Option<(u64, Py<PyBytes>)>> {
        let inner = self.inner.clone();
        let rt = runtime();
        let res = py.allow_threads(|| rt.block_on(async move { inner.load(&pid).await }));
        Ok(res.map(|(meta, payload)| (meta.sequence_nr, PyBytes::new_bound(py, &payload).unbind())))
    }

    /// Delete snapshots for `pid` whose `sequence_nr` is `<= to_seq`.
    fn delete(&self, py: Python<'_>, pid: String, to_seq: u64) -> PyResult<()> {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| rt.block_on(async move { inner.delete(&pid, to_seq).await }));
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------
// RecoveryPermitter
// ---------------------------------------------------------------------

#[pyclass(name = "RecoveryPermitter", module = "atomr._native.persistence")]
pub struct PyRecoveryPermitter {
    inner: RecoveryPermitter,
}

#[pymethods]
impl PyRecoveryPermitter {
    #[new]
    #[pyo3(signature = (max_concurrent=50))]
    fn new(max_concurrent: usize) -> PyResult<Self> {
        if max_concurrent == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("max_concurrent must be >= 1"));
        }
        Ok(Self { inner: RecoveryPermitter::new(max_concurrent) })
    }

    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn available(&self) -> usize {
        self.inner.available()
    }

    fn in_flight(&self) -> usize {
        self.inner.in_flight()
    }

    /// Block (synchronously, on the shared Tokio runtime) until a
    /// permit is available. The permit is released as soon as this
    /// method returns — Python-side recovery is sequential per actor
    /// so we don't need to hold a permit across multiple calls; the
    /// gate's purpose is to bound concurrent journal-recovery storms.
    fn acquire_blocking(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        let rt = runtime();
        let ok = py.allow_threads(|| rt.block_on(async move { inner.acquire().await.is_some() }));
        if ok {
            Ok(())
        } else {
            Err(PyErr::new::<errors::AtomrError, _>("recovery permitter closed"))
        }
    }

    fn close(&self) {
        self.inner.close();
    }
}

// ---------------------------------------------------------------------
// Effect — return type from command_handler.
// ---------------------------------------------------------------------

/// Effect kind. Each variant maps to a small payload pulled from
/// Python. We deliberately keep this an opaque pyclass so users can
/// only construct effects through the documented constructors.
#[pyclass(name = "Effect", module = "atomr._native.persistence")]
pub struct PyEffect {
    #[pyo3(get)]
    kind: String,
    /// Persisted event(s) — populated for `persist` / `persist_all`.
    /// Always a list (single-event variant wraps a one-element list).
    #[pyo3(get)]
    events: Option<Py<PyAny>>,
    /// Snapshot cadence. `Some(0)` means snapshot now;
    /// `Some(n)` for `n > 0` means snapshot every `n` events.
    #[pyo3(get)]
    every: Option<u64>,
    /// Reply payload — populated by `Effect.reply_message(value)`.
    ///
    /// Originally the constructor was named `Effect.reply` and the
    /// field surfaced as `reply_value`, but PyO3 collides a
    /// staticmethod and a getter under the same name when the
    /// staticmethod-name (`reply`) matches a prefix of any other
    /// member. The clean fix renames the constructor to
    /// `reply_message` and exposes the field under the natural
    /// `value`. See `python/atomr/persistence.py` for the full
    /// migration note.
    #[pyo3(get, name = "value")]
    reply_payload: Option<Py<PyAny>>,
}

#[pymethods]
impl PyEffect {
    /// Persist a single event.
    #[staticmethod]
    fn persist(py: Python<'_>, event: Py<PyAny>) -> PyResult<Self> {
        let list = PyList::empty_bound(py);
        list.append(event)?;
        Ok(Self {
            kind: "persist".into(),
            events: Some(list.into_any().unbind()),
            every: None,
            reply_payload: None,
        })
    }

    /// Persist a batch of events atomically.
    #[staticmethod]
    fn persist_all(py: Python<'_>, events: Py<PyAny>) -> PyResult<Self> {
        // Validate that `events` is iterable. We re-materialise into a
        // list so the Python EventSourcedActor can iterate cheaply.
        let bound = events.bind(py);
        let list = PyList::empty_bound(py);
        for item in bound.iter()? {
            list.append(item?)?;
        }
        Ok(Self {
            kind: "persist".into(),
            events: Some(list.into_any().unbind()),
            every: None,
            reply_payload: None,
        })
    }

    /// Snapshot now (default) or set the periodic cadence.
    ///
    /// * `Effect.snapshot()` — snapshot at the next quiescent point.
    /// * `Effect.snapshot(every=N)` — set a cadence: snapshot every
    ///   `N` events from this point on.
    #[staticmethod]
    #[pyo3(signature = (every=None))]
    fn snapshot(every: Option<u64>) -> Self {
        Self { kind: "snapshot".into(), events: None, every, reply_payload: None }
    }

    /// Reply to the sender of the current command.
    ///
    /// Note: this used to be `Effect.reply(value)`, but the name
    /// collided with the field that exposes the payload back to
    /// Python. The constructor is now `Effect.reply_message(value)`
    /// and the payload is read as `effect.value`.
    #[staticmethod]
    fn reply_message(value: Py<PyAny>) -> Self {
        Self { kind: "reply".into(), events: None, every: None, reply_payload: Some(value) }
    }

    /// Stop the actor after the current command finishes.
    #[staticmethod]
    fn stop() -> Self {
        Self { kind: "stop".into(), events: None, every: None, reply_payload: None }
    }

    /// No-op effect — used as a sentinel by helpers that want to
    /// always return *something*.
    #[staticmethod]
    fn none() -> Self {
        Self { kind: "none".into(), events: None, every: None, reply_payload: None }
    }

    fn __repr__(&self) -> String {
        format!(
            "Effect(kind={}, every={:?}, has_reply={}, has_events={})",
            self.kind,
            self.every,
            self.reply_payload.is_some(),
            self.events.is_some()
        )
    }
}

// ---------------------------------------------------------------------
// Module registration.
// ---------------------------------------------------------------------

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "persistence")?;
    sub.add_class::<PyInMemoryJournal>()?;
    sub.add_class::<PyInMemorySnapshotStore>()?;
    sub.add_class::<PyRecoveryPermitter>()?;
    sub.add_class::<PyEffect>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
