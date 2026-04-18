//! `Config` binding.

use pyo3::prelude::*;

use rustakka_config::Config;

#[pyclass(name = "Config", module = "rustakka._native", frozen)]
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
            .map_err(|e| PyErr::new::<crate::errors::RustakkaError, _>(e.to_string()))?;
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
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConfig>()
}
