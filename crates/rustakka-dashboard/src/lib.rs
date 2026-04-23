//! # rustakka-dashboard
//!
//! Optional HTTP + WebSocket service exposing a [`rustakka_telemetry`]
//! snapshot surface plus a live event stream. Hosts the embedded React
//! single-page application when built with `--features embed-ui`.
//!
//! ```no_run
//! use std::sync::Arc;
//! use rustakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
//! use rustakka_telemetry::TelemetryExtension;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let telemetry = TelemetryExtension::new("node", 1024);
//! let server = DashboardServer::new(
//!     telemetry.clone(),
//!     DashboardConfig {
//!         bind: "127.0.0.1:9100".parse()?,
//!         mode: DashboardMode::Local,
//!         ..Default::default()
//!     },
//! );
//! let handle = server.start().await?;
//! // ...application runs...
//! handle.shutdown().await;
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod aggregator;
pub mod exporters;
pub mod routes;
pub mod spa;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use rustakka_telemetry::TelemetryExtension;

/// Where to get telemetry from when serving requests.
#[derive(Clone, Debug, Default)]
pub enum DashboardMode {
    /// Single-node: use the in-process telemetry extension.
    #[default]
    Local,
    /// Fan out to peer dashboards and merge their responses. Requires
    /// the `aggregator` cargo feature.
    Cluster { peers: Vec<String> },
}

#[derive(Clone, Debug)]
pub struct DashboardConfig {
    pub bind: SocketAddr,
    pub mode: DashboardMode,
    pub ws_channel_capacity: usize,
    pub exporters: rustakka_telemetry::exporters::config::ExportersConfig,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9100".parse().unwrap(),
            mode: DashboardMode::Local,
            ws_channel_capacity: 1024,
            exporters: Default::default(),
        }
    }
}

/// Shared router state. Wrapped in `Arc` and cloned into handlers.
#[derive(Clone)]
pub struct AppState {
    pub telemetry: Arc<TelemetryExtension>,
    pub mode: DashboardMode,
    pub exporters: exporters::ExporterHandles,
}

impl AppState {
    pub fn new(telemetry: Arc<TelemetryExtension>, mode: DashboardMode) -> Self {
        Self { telemetry, mode, exporters: Default::default() }
    }
}

/// Running dashboard service handle. Drop to leave running; call
/// [`Self::shutdown`] to stop gracefully.
pub struct DashboardHandle {
    pub bound_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl DashboardHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

pub struct DashboardServer {
    telemetry: Arc<TelemetryExtension>,
    config: DashboardConfig,
}

impl DashboardServer {
    pub fn new(telemetry: Arc<TelemetryExtension>, config: DashboardConfig) -> Self {
        Self { telemetry, config }
    }

    /// Build the axum router. Public so tests can exercise handlers via
    /// `tower::ServiceExt::oneshot` without binding a real socket.
    pub fn router(&self) -> Router {
        let state = AppState::new(self.telemetry.clone(), self.config.mode.clone());
        routes::build_router(state, self.config.ws_channel_capacity)
    }

    /// Build the router and also apply exporters so `/metrics` can render.
    /// Primarily used for integration tests that need to exercise both.
    pub fn router_with_exporters(&self) -> Result<Router, ServerError> {
        let handles = exporters::apply(&self.telemetry, &self.config.exporters)?;
        let mut state = AppState::new(self.telemetry.clone(), self.config.mode.clone());
        state.exporters = handles;
        Ok(routes::build_router(state, self.config.ws_channel_capacity))
    }

    /// Bind and start serving. Applies configured exporters before
    /// starting the HTTP server.
    pub async fn start(self) -> Result<DashboardHandle, ServerError> {
        let handles = exporters::apply(&self.telemetry, &self.config.exporters)?;
        let mut state = AppState::new(self.telemetry.clone(), self.config.mode.clone());
        state.exporters = handles;
        let router = routes::build_router(state, self.config.ws_channel_capacity);
        let listener = tokio::net::TcpListener::bind(self.config.bind)
            .await
            .map_err(ServerError::Bind)?;
        let bound = listener.local_addr().map_err(ServerError::Bind)?;
        let (tx, rx) = oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, router.into_make_service())
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Ok(DashboardHandle { bound_addr: bound, shutdown_tx: Some(tx), join: Some(join) })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to bind: {0}")]
    Bind(std::io::Error),
    #[error("exporter init failed: {0}")]
    Exporter(String),
}
