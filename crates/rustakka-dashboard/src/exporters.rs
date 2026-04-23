//! Thin adapter — instantiates the `rustakka-telemetry` exporters the
//! configuration asks for. Every exporter is cargo-feature gated; when a
//! feature is disabled we return a descriptive error so the user sees a
//! clean message instead of a silent drop.

use std::sync::Arc;

use rustakka_telemetry::exporters::config::ExportersConfig;
use rustakka_telemetry::TelemetryExtension;

use crate::ServerError;

/// Handles returned from [`apply`]. Any that were enabled get cloned into
/// [`crate::AppState`] so HTTP handlers can render their output on demand.
#[derive(Default, Clone)]
pub struct ExporterHandles {
    #[cfg(feature = "metrics-prometheus")]
    pub prometheus: Option<
        Arc<rustakka_telemetry::exporters::prometheus::PrometheusExporter>,
    >,
}

pub fn apply(
    telemetry: &Arc<TelemetryExtension>,
    cfg: &ExportersConfig,
) -> Result<ExporterHandles, ServerError> {
    let mut handles = ExporterHandles::default();
    if let Some(prom) = &cfg.prometheus {
        if prom.enabled {
            apply_prometheus(telemetry, prom, &mut handles)?;
        }
    }
    if let Some(otlp) = &cfg.otlp {
        if otlp.enabled {
            apply_otlp(telemetry, otlp)?;
        }
    }
    Ok(handles)
}

#[cfg(feature = "metrics-prometheus")]
fn apply_prometheus(
    telemetry: &Arc<TelemetryExtension>,
    cfg: &rustakka_telemetry::exporters::config::PrometheusConfig,
    handles: &mut ExporterHandles,
) -> Result<(), ServerError> {
    let exp = rustakka_telemetry::exporters::prometheus::PrometheusExporter::with_namespace(
        telemetry.node.clone(),
        cfg.namespace.as_deref(),
    )
    .map_err(|e| ServerError::Exporter(format!("prometheus init: {e}")))?;
    exp.seed_from_snapshot(&telemetry.snapshot());
    let arc = Arc::new(exp);
    telemetry.add_exporter(arc.clone());
    handles.prometheus = Some(arc);
    Ok(())
}

#[cfg(not(feature = "metrics-prometheus"))]
fn apply_prometheus(
    _telemetry: &Arc<TelemetryExtension>,
    _cfg: &rustakka_telemetry::exporters::config::PrometheusConfig,
    _handles: &mut ExporterHandles,
) -> Result<(), ServerError> {
    Err(ServerError::Exporter(
        "prometheus exporter requested but the `metrics-prometheus` feature is disabled"
            .into(),
    ))
}

#[cfg(feature = "metrics-otel")]
fn apply_otlp(
    telemetry: &Arc<TelemetryExtension>,
    cfg: &rustakka_telemetry::exporters::config::OtlpConfig,
) -> Result<(), ServerError> {
    let exporter = rustakka_telemetry::exporters::otel::OtelExporter::new_with_node(
        cfg.clone(),
        telemetry.node.clone(),
    )
    .map_err(|e| ServerError::Exporter(format!("otel init: {e}")))?;
    telemetry.add_exporter(Arc::new(exporter));
    Ok(())
}

#[cfg(not(feature = "metrics-otel"))]
fn apply_otlp(
    _telemetry: &Arc<TelemetryExtension>,
    _cfg: &rustakka_telemetry::exporters::config::OtlpConfig,
) -> Result<(), ServerError> {
    Err(ServerError::Exporter(
        "otlp exporter requested but the `metrics-otel` feature is disabled".into(),
    ))
}
