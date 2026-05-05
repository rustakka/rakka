//! Persistence submodule (Phase P7 slice).

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use atomr_persistence::{InMemoryJournal, Journal, PersistentRepr};

use crate::errors;
use crate::runtime::runtime;

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

    fn highest_sequence_nr(&self, py: Python<'_>, pid: String) -> PyResult<u64> {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move { inner.highest_sequence_nr(&pid, 0).await.map_err(errors::map) })
        })
    }

    /// Replay all events tagged with `tag` starting at `from_offset`.
    /// Returns a list of `(persistence_id, sequence_nr, payload, tags)`
    /// tuples. akka.net: `IEventsByTagQuery`.
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
            rt.block_on(async move {
                inner.events_by_tag(&tag, from_offset, max).await.map_err(errors::map)
            })
        })?;
        let list = PyList::empty_bound(py);
        for r in reprs {
            let tup: Py<PyAny> = (
                r.persistence_id,
                r.sequence_nr,
                PyBytes::new_bound(py, &r.payload).unbind(),
                r.tags,
            )
                .into_py(py);
            list.append(tup)?;
        }
        Ok(list)
    }

    /// Return distinct persistence ids known to the journal.
    /// akka.net: `IPersistenceIdsQuery.AllPersistenceIds`.
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

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "persistence")?;
    sub.add_class::<PyInMemoryJournal>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
