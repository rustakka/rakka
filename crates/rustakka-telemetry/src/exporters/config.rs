//! Runtime configuration for exporters. Safe to parse even when the
//! corresponding exporter cargo features are disabled — applying an
//! unknown exporter returns an error rather than panicking.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExportersConfig {
    #[serde(default)]
    pub prometheus: Option<PrometheusConfig>,
    #[serde(default)]
    pub otlp: Option<OtlpConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self { namespace: None, enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// OTLP collector endpoint (e.g. `http://localhost:4317` for gRPC or
    /// `http://localhost:4318` for HTTP).
    pub endpoint: String,
    /// Transport protocol: `grpc` or `http`.
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// Service name attached as the `service.name` resource attribute.
    #[serde(default)]
    pub service_name: Option<String>,
    /// Metric push interval in seconds.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Optional headers (e.g. auth tokens).
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// Extra resource attributes beyond `service.name`.
    #[serde(default)]
    pub resource_attributes: std::collections::HashMap<String, String>,
    /// Emit traces (spans) in addition to metrics.
    #[serde(default = "default_true")]
    pub traces: bool,
    /// Use the stdout exporter instead of OTLP. Useful for dev/tests.
    #[serde(default)]
    pub stdout: bool,
}

fn default_protocol() -> String {
    "grpc".into()
}

fn default_interval() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config_from_toml() {
        let toml = r#"
            [prometheus]
            namespace = "rustakka"

            [otlp]
            endpoint = "http://collector:4317"
            protocol = "grpc"
            service_name = "my-service"
            interval_secs = 15
            stdout = false

            [otlp.headers]
            authorization = "Bearer x"
        "#;
        let parsed: ExportersConfig = toml::from_str(toml).unwrap();
        assert_eq!(parsed.prometheus.unwrap().namespace.unwrap(), "rustakka");
        let otlp = parsed.otlp.unwrap();
        assert_eq!(otlp.endpoint, "http://collector:4317");
        assert_eq!(otlp.protocol, "grpc");
        assert_eq!(otlp.headers.get("authorization").unwrap(), "Bearer x");
    }
}
