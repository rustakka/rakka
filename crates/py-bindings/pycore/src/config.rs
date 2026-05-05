//! `Config` binding.

use pyo3::prelude::*;

use atomr_config::Config;

#[pyclass(name = "Config", module = "atomr._native", frozen)]
pub struct PyConfig {
    pub(crate) inner: Config,
}

#[pymethods]
impl PyConfig {
    #[staticmethod]
    pub fn empty() -> Self {
        Self { inner: Config::empty() }
    }

    #[staticmethod]
    pub fn from_toml(text: &str) -> PyResult<Self> {
        let inner = Config::from_toml_str(text)
            .map_err(|e| PyErr::new::<crate::errors::AtomrError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.inner.get_string(key).ok()
    }

    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.inner.get_int(key).ok()
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.inner.get_bool(key).ok()
    }

    /// Deserialize the subtree at `key` into a Python object via JSON.
    /// Mirrors the typed `Config::extract<T>` Rust API: the subtree is
    /// converted to JSON, parsed, and returned as the natural Python
    /// dict/list/scalar tree. akka.net: `Config.GetConfig(...).Get<T>()`
    /// then JSON serialize for the cross-language bridge.
    pub fn extract<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        let value: serde_json::Value = self
            .inner
            .extract(key)
            .map_err(|e| PyErr::new::<crate::errors::AtomrError, _>(e.to_string()))?;
        json_to_py(py, &value)
    }

    /// Deserialize the entire config tree into a Python object via JSON.
    pub fn extract_root<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let value: serde_json::Value = self
            .inner
            .extract_root()
            .map_err(|e| PyErr::new::<crate::errors::AtomrError, _>(e.to_string()))?;
        json_to_py(py, &value)
    }
}

fn json_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::types::{PyDict, PyList};
    Ok(match value {
        serde_json::Value::Null => py.None().into_bound(py),
        serde_json::Value::Bool(b) => b.into_py(py).into_bound(py),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py(py).into_bound(py)
            } else if let Some(u) = n.as_u64() {
                u.into_py(py).into_bound(py)
            } else if let Some(f) = n.as_f64() {
                f.into_py(py).into_bound(py)
            } else {
                py.None().into_bound(py)
            }
        }
        serde_json::Value::String(s) => s.clone().into_py(py).into_bound(py),
        serde_json::Value::Array(items) => {
            let list = PyList::empty_bound(py);
            for v in items {
                list.append(json_to_py(py, v)?)?;
            }
            list.into_any()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            dict.into_any()
        }
    })
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConfig>()
}
