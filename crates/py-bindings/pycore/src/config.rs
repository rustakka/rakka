//! `Config` binding.
//!
//! Accepts arbitrary keys and round-trips them through the underlying
//! `atomr_config::Config` value tree. The Phase-5 cluster control plane
//! reads `cluster.sbr.strategy` and friends straight off this object;
//! see `ext_cluster.rs` for the supported keys.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

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

    /// Build a `Config` from a Python `dict` (nested dicts/lists/scalars
    /// are supported). Convenient for setting cluster keys without
    /// constructing TOML by hand:
    ///
    /// ```python
    /// cfg = Config.from_dict({"cluster": {"sbr": {"strategy": "keep-majority"}}})
    /// ```
    #[staticmethod]
    pub fn from_dict(py: Python<'_>, mapping: Bound<'_, PyAny>) -> PyResult<Self> {
        let value = py_to_json(py, &mapping)?;
        // Round-trip via TOML to leverage the existing Config parser
        // and its merge semantics.
        let toml_text = json_to_toml(&value)?;
        let inner = Config::from_toml_str(&toml_text)
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
    /// dict/list/scalar tree.
    /// then JSON serialize for the cross-language bridge.
    pub fn extract<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        let value: serde_json::Value =
            self.inner.extract(key).map_err(|e| PyErr::new::<crate::errors::AtomrError, _>(e.to_string()))?;
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

fn py_to_json(_py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(serde_json::Value::Number(i.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(serde_json::Value::String(s));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(obj.py(), &v)?);
        }
        return Ok(serde_json::Value::Object(map));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json(obj.py(), &item)?);
        }
        return Ok(serde_json::Value::Array(arr));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "unsupported config value type: {}",
        obj.get_type().name()?
    )))
}

fn json_to_toml(v: &serde_json::Value) -> PyResult<String> {
    let toml_value = json_to_toml_value(v)?;
    let table = match toml_value {
        toml::Value::Table(t) => t,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Config.from_dict: top-level value must be a mapping",
            ));
        }
    };
    toml::to_string(&table).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn json_to_toml_value(v: &serde_json::Value) -> PyResult<toml::Value> {
    Ok(match v {
        serde_json::Value::Null => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Config.from_dict: TOML cannot represent null values",
            ))
        }
        serde_json::Value::Bool(b) => toml::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                toml::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                toml::Value::Float(f)
            } else {
                return Err(pyo3::exceptions::PyValueError::new_err("unsupported number"));
            }
        }
        serde_json::Value::String(s) => toml::Value::String(s.clone()),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(json_to_toml_value(it)?);
            }
            toml::Value::Array(out)
        }
        serde_json::Value::Object(map) => {
            let mut out = toml::map::Map::new();
            for (k, vv) in map {
                out.insert(k.clone(), json_to_toml_value(vv)?);
            }
            toml::Value::Table(out)
        }
    })
}

fn json_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
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
