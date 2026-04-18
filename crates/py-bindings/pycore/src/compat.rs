//! C-extension compatibility registry. Records which modules are known to
//! be safe for subinterpreter or no-GIL execution, and offers a probe used
//! at `ActorSystem.create` time.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

#[derive(Clone, Debug)]
pub struct CompatFlags {
    pub subinterpreter_safe: bool,
    pub nogil_safe: bool,
    pub notes: String,
}

static REGISTRY: Lazy<RwLock<HashMap<String, CompatFlags>>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // Baseline, conservative defaults — operators override at import time.
    let yes =
        |notes: &str| CompatFlags { subinterpreter_safe: true, nogil_safe: true, notes: notes.into() };
    let sub_only = |notes: &str| CompatFlags {
        subinterpreter_safe: true,
        nogil_safe: false,
        notes: notes.into(),
    };
    let unknown =
        |notes: &str| CompatFlags { subinterpreter_safe: false, nogil_safe: false, notes: notes.into() };

    m.insert("json".into(), yes("stdlib"));
    m.insert("dataclasses".into(), yes("stdlib"));
    m.insert("typing".into(), yes("stdlib"));
    m.insert("asyncio".into(), yes("stdlib"));
    m.insert("collections".into(), yes("stdlib"));
    m.insert("pickle".into(), yes("stdlib"));
    m.insert("msgpack".into(), sub_only("C ext, needs per-release audit"));
    m.insert("orjson".into(), sub_only("C ext, needs per-release audit"));
    m.insert("numpy".into(), sub_only("core OK; some module-level state"));
    m.insert("pydantic".into(), sub_only("compiled backend varies"));
    m.insert("pandas".into(), unknown("heavy module-level state"));
    m.insert("torch".into(), unknown("CUDA ctx tied to process/thread"));
    RwLock::new(m)
});

pub fn declare(name: &str, flags: CompatFlags) {
    REGISTRY.write().insert(name.into(), flags);
}

pub fn get(name: &str) -> Option<CompatFlags> {
    REGISTRY.read().get(name).cloned()
}

#[pyfunction(name = "declare_compat")]
#[pyo3(signature = (name, subinterpreter_safe=false, nogil_safe=false, notes=String::new()))]
fn declare_py(name: String, subinterpreter_safe: bool, nogil_safe: bool, notes: String) {
    declare(&name, CompatFlags { subinterpreter_safe, nogil_safe, notes });
}

#[pyfunction(name = "compat_flags")]
fn compat_flags<'py>(py: Python<'py>, name: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
    let flags = match get(name) {
        Some(f) => f,
        None => return Ok(None),
    };
    let d = PyDict::new_bound(py);
    d.set_item("subinterpreter_safe", flags.subinterpreter_safe)?;
    d.set_item("nogil_safe", flags.nogil_safe)?;
    d.set_item("notes", flags.notes)?;
    Ok(Some(d))
}

#[pyfunction(name = "compat_list")]
fn compat_list(py: Python<'_>) -> PyResult<Bound<'_, PyList>> {
    let out = PyList::empty_bound(py);
    let guard = REGISTRY.read();
    let mut entries: Vec<_> = guard.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (name, flags) in entries {
        let d = PyDict::new_bound(py);
        d.set_item("name", name)?;
        d.set_item("subinterpreter_safe", flags.subinterpreter_safe)?;
        d.set_item("nogil_safe", flags.nogil_safe)?;
        d.set_item("notes", &flags.notes)?;
        out.append(d)?;
    }
    Ok(out)
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(declare_py, m)?)?;
    m.add_function(wrap_pyfunction!(compat_flags, m)?)?;
    m.add_function(wrap_pyfunction!(compat_list, m)?)?;
    Ok(())
}
