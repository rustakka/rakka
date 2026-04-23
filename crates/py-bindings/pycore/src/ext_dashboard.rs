//! Python bindings for `rustakka-dashboard`.
//!
//! Exposes a single factory — `start(...)` — that stands up the axum
//! dashboard server on a background tokio runtime and returns a
//! `DashboardHandle` the caller drops (or explicitly `shutdown()`s) to
//! stop the service.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use rustakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use rustakka_telemetry::exporters::config::{ExportersConfig, OtlpConfig, PrometheusConfig};
use rustakka_telemetry::TelemetryExtension;

use crate::errors;
use crate::runtime::runtime;

#[pyclass(name = "DashboardHandle", module = "rustakka._native.dashboard")]
pub struct PyDashboardHandle {
    bound_addr: String,
    inner: Option<rustakka_dashboard::DashboardHandle>,
}

#[pymethods]
impl PyDashboardHandle {
    #[getter]
    fn address(&self) -> &str {
        &self.bound_addr
    }

    fn shutdown(&mut self, py: Python<'_>) {
        if let Some(h) = self.inner.take() {
            let rt = runtime();
            py.allow_threads(|| rt.block_on(async move { h.shutdown().await }));
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<()> {
        self.shutdown(py);
        Ok(())
    }
}

fn parse_exporters(dict: Option<&Bound<'_, PyDict>>) -> PyResult<ExportersConfig> {
    let Some(d) = dict else {
        return Ok(ExportersConfig::default());
    };
    let mut out = ExportersConfig::default();
    if let Some(prom) = d.get_item("prometheus")? {
        if let Ok(enabled) = prom.extract::<bool>() {
            out.prometheus = Some(PrometheusConfig { enabled, ..Default::default() });
        } else {
            let sub: &Bound<'_, PyDict> = prom.downcast()?;
            let enabled = sub
                .get_item("enabled")?
                .map(|v| v.extract::<bool>())
                .transpose()?
                .unwrap_or(true);
            let namespace = sub
                .get_item("namespace")?
                .map(|v| v.extract::<String>())
                .transpose()?;
            out.prometheus = Some(PrometheusConfig { enabled, namespace });
        }
    }
    if let Some(otlp) = d.get_item("otlp")? {
        let sub: &Bound<'_, PyDict> = otlp.downcast()?;
        let endpoint = sub
            .get_item("endpoint")?
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                "exporters.otlp requires `endpoint`",
            ))?
            .extract::<String>()?;
        let protocol = sub
            .get_item("protocol")?
            .map(|v| v.extract::<String>())
            .transpose()?
            .unwrap_or_else(|| "grpc".into());
        let service_name = sub
            .get_item("service_name")?
            .map(|v| v.extract::<String>())
            .transpose()?;
        let interval_secs = sub
            .get_item("interval_secs")?
            .map(|v| v.extract::<u64>())
            .transpose()?
            .unwrap_or(30);
        let stdout = sub
            .get_item("stdout")?
            .map(|v| v.extract::<bool>())
            .transpose()?
            .unwrap_or(false);
        let headers: HashMap<String, String> = sub
            .get_item("headers")?
            .map(|v| v.extract::<HashMap<String, String>>())
            .transpose()?
            .unwrap_or_default();
        let resource_attributes: HashMap<String, String> = sub
            .get_item("resource_attributes")?
            .map(|v| v.extract::<HashMap<String, String>>())
            .transpose()?
            .unwrap_or_default();
        out.otlp = Some(OtlpConfig {
            enabled: true,
            endpoint,
            protocol,
            service_name,
            interval_secs,
            headers,
            resource_attributes,
            traces: true,
            stdout,
        });
    }
    Ok(out)
}

/// `serve(bind="127.0.0.1:9100", node="local", peers=None, exporters=None)`
/// — start the dashboard server and return a `DashboardHandle`.
///
/// The service runs on the shared PyO3 tokio runtime. Call
/// `handle.shutdown()` to stop it; dropping the handle without calling
/// `shutdown()` leaves the server running until interpreter exit.
#[pyfunction]
#[pyo3(signature = (bind="127.0.0.1:9100".into(), node="local".into(), peers=None, exporters=None))]
fn serve(
    py: Python<'_>,
    bind: String,
    node: String,
    peers: Option<Vec<String>>,
    exporters: Option<Py<PyDict>>,
) -> PyResult<Py<PyDashboardHandle>> {
    let bind_addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e: std::net::AddrParseError| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid bind address {bind:?}: {e}"))
        })?;
    let mode = match peers {
        None => DashboardMode::Local,
        Some(ps) if ps.is_empty() => DashboardMode::Local,
        Some(ps) => DashboardMode::Cluster { peers: ps },
    };
    let exporters_cfg = {
        let bound = exporters.as_ref().map(|p| p.bind(py));
        parse_exporters(bound)?
    };
    let telemetry = TelemetryExtension::new(node.clone(), 1024);
    let cfg = DashboardConfig {
        bind: bind_addr,
        mode,
        ws_channel_capacity: 1024,
        exporters: exporters_cfg,
    };
    let server = DashboardServer::new(telemetry, cfg);
    let rt = runtime();
    let handle = py
        .allow_threads(|| rt.block_on(async move { server.start().await }))
        .map_err(errors::map)?;
    let bound = handle.bound_addr.to_string();
    Py::new(
        py,
        PyDashboardHandle {
            bound_addr: bound,
            inner: Some(handle),
        },
    )
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "dashboard")?;
    sub.add_class::<PyDashboardHandle>()?;
    sub.add_function(wrap_pyfunction!(serve, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
