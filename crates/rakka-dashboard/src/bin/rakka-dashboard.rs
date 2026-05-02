//! Standalone `rakka-dashboard` binary. Launches a telemetry-backed
//! axum server without a host actor system; useful for operators who
//! want to aggregate across peers or serve a pre-populated telemetry
//! extension.

use std::net::SocketAddr;

use clap::Parser;
use rakka_dashboard::{DashboardConfig, DashboardMode, DashboardServer};
use rakka_telemetry::exporters::config::{ExportersConfig, OtlpConfig, PrometheusConfig};
use rakka_telemetry::TelemetryExtension;

#[derive(Parser, Debug)]
#[command(name = "rakka-dashboard", about = "Serve the rakka telemetry dashboard", version)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9100")]
    bind: SocketAddr,

    #[arg(long, default_value = "local")]
    node: String,

    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,

    #[arg(long)]
    prometheus: bool,

    #[arg(long)]
    otlp_endpoint: Option<String>,

    #[arg(long, default_value = "grpc")]
    otlp_protocol: String,

    #[arg(long)]
    otel_service_name: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_env_filter("info").try_init().ok();

    let telemetry = TelemetryExtension::new(args.node.clone(), 1024);

    let exporters = ExportersConfig {
        prometheus: args.prometheus.then(|| PrometheusConfig { enabled: true, ..Default::default() }),
        otlp: args.otlp_endpoint.clone().map(|endpoint| OtlpConfig {
            enabled: true,
            endpoint,
            protocol: args.otlp_protocol.clone(),
            service_name: args.otel_service_name.clone(),
            interval_secs: 30,
            headers: Default::default(),
            resource_attributes: Default::default(),
            traces: true,
            stdout: false,
        }),
    };

    let mode = if args.peers.is_empty() {
        DashboardMode::Local
    } else {
        DashboardMode::Cluster { peers: args.peers.clone() }
    };

    let cfg = DashboardConfig { bind: args.bind, mode, ws_channel_capacity: 1024, exporters };
    let server = DashboardServer::new(telemetry, cfg);
    let handle = server.start().await?;
    tracing::info!(addr = %handle.bound_addr, "dashboard listening");

    tokio::signal::ctrl_c().await?;
    handle.shutdown().await;
    Ok(())
}
