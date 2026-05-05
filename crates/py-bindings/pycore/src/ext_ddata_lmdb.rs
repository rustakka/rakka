//! redb-backed durable distributed-data store. akka.net analog:
//! `Akka.DistributedData.LightningDB.LmdbDurableStore`.
//!
//! Implementation note: the spec requested a separate `pyddata-lmdb`
//! py-binding crate, but pycore is currently the only binding crate
//! actually loaded into the `_native` extension module. To keep wiring
//! simple we expose the same API as a `ddata_lmdb` submodule under
//! pycore. The Python facade lives at `python/atomr/ddata_lmdb.py`.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use atomr_distributed_data::DurableStore;
use atomr_distributed_data_lmdb::RedbDurableStore;

use crate::errors;

#[pyclass(name = "RedbDurableStore", module = "atomr._native.ddata_lmdb")]
pub struct PyRedbDurableStore {
    inner: RedbDurableStore,
}

#[pymethods]
impl PyRedbDurableStore {
    /// Open or create a redb-backed durable store at `path`.
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let inner = RedbDurableStore::open(PathBuf::from(path)).map_err(errors::map)?;
        Ok(Self { inner })
    }

    /// Convenience constructor that opens a fresh database in `tempdir`.
    #[staticmethod]
    fn tmp() -> PyResult<Self> {
        let inner = RedbDurableStore::tmp().map_err(errors::map)?;
        Ok(Self { inner })
    }

    /// Path the database file lives at on disk.
    #[getter]
    fn path(&self) -> String {
        self.inner.path().display().to_string()
    }

    /// Persist `value` under `key`.
    fn persist(&self, key: String, value: Bound<'_, PyBytes>) -> PyResult<()> {
        self.inner.persist(&key, value.as_bytes()).map_err(errors::map)
    }

    /// Load the value for `key`, or `None` if absent / deleted.
    fn load<'py>(&self, py: Python<'py>, key: String) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let v = self.inner.load(&key).map_err(errors::map)?;
        Ok(v.map(|b| PyBytes::new_bound(py, &b)))
    }

    /// Drop the entry for `key`. No-op if the key is absent.
    fn delete_marker(&self, key: String) -> PyResult<()> {
        self.inner.delete_marker(&key).map_err(errors::map)
    }

    /// Sorted snapshot of all keys currently in the store.
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let ks = self.inner.keys().map_err(errors::map)?;
        let list = PyList::empty_bound(py);
        for k in ks {
            list.append(k)?;
        }
        Ok(list)
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "ddata_lmdb")?;
    sub.add_class::<PyRedbDurableStore>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
