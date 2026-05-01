//! `RemoteSettings`. akka.net: `Remote/RemoteSettings.cs`.
//!
//! Knobs for the remoting subsystem. Defaults are conservative and
//! suitable for development; production deployments should tune the
//! frame size, timeouts, and quarantine windows from `rakka-config`.

use std::time::Duration;

use rakka_config::Config;

#[derive(Debug, Clone)]
pub struct RemoteSettings {
    /// Wire transport scheme — by default `"akka.tcp"`.
    pub protocol: String,
    /// Local hostname to advertise. `None` means bind-only.
    pub hostname: Option<String>,
    /// Local TCP port to bind. `0` lets the OS pick.
    pub port: u16,
    /// Maximum frame size in bytes (length-prefix + payload).
    pub max_frame_size: usize,
    /// How often the writer emits a `Heartbeat` PDU when idle.
    pub heartbeat_interval: Duration,
    /// Heartbeat absence after which an endpoint is considered failing.
    pub heartbeat_timeout: Duration,
    /// Hard cap on the time we wait for the `Associate` handshake reply.
    pub handshake_timeout: Duration,
    /// Time window during which a quarantined remote may not reassociate.
    pub quarantine_duration: Duration,
    /// Initial backoff for endpoint reconnect attempts.
    pub backoff_initial: Duration,
    /// Cap on reconnect backoff (with jitter).
    pub backoff_max: Duration,
    /// Reconnect backoff multiplier per attempt.
    pub backoff_multiplier: f64,
    /// Number of reconnect attempts before giving up.
    pub max_reconnect_attempts: u32,
    /// Sliding-window size for ack'd delivery.
    pub ack_window: u32,
    /// `Send` buffer length (per-endpoint, bounded mpsc).
    pub send_buffer_len: usize,
    /// Default serializer id used for outbound messages whose type does
    /// not have a more specific serializer registered.
    pub default_serializer_id: u32,
    /// Cookie required during handshake. `None` disables cookie auth.
    pub require_cookie: Option<String>,
    /// Watch heartbeat tick interval (RemoteWatcher).
    pub watch_heartbeat_interval: Duration,
    /// Watch failure threshold (in missed heartbeats).
    pub watch_failure_threshold: u32,

    // -- Phase 5.J: phi-accrual failure-detector tuning --
    //
    // These mirror akka.net's `akka.remote.watch-failure-detector.*`
    // keys. Producing a `FailureDetectorRegistry` from `RemoteSettings`
    // honours each knob.
    /// φ value above which the peer is considered failed (akka.net
    /// default: 8.0 for watch, 10.0 for cluster).
    pub phi_threshold: f64,
    /// Maximum sample size kept in the heart-beat history.
    pub phi_max_sample_size: usize,
    /// Floor on the inter-arrival std-dev (avoids over-confidence on
    /// suspiciously stable links). akka.net default: 100ms.
    pub phi_min_std_deviation: Duration,
    /// Pause window the detector tolerates before suspicion grows.
    pub phi_acceptable_heartbeat_pause: Duration,

    /// TLS configuration. Default is unconfigured (`!enabled()`).
    /// Phase 5.E.
    pub tls: crate::tls::TlsConfig,

    /// Maximum payload bytes per wire frame. Larger payloads are
    /// fragmented via `chunking::Chunker`. Phase 5.F.
    pub maximum_payload_size: usize,
}

impl Default for RemoteSettings {
    fn default() -> Self {
        Self {
            protocol: "akka.tcp".into(),
            hostname: None,
            port: 0,
            max_frame_size: 4 * 1024 * 1024,
            heartbeat_interval: Duration::from_millis(1000),
            heartbeat_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(15),
            quarantine_duration: Duration::from_secs(60),
            backoff_initial: Duration::from_millis(200),
            backoff_max: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            max_reconnect_attempts: 10,
            ack_window: 1000,
            send_buffer_len: 4096,
            default_serializer_id: crate::serialization::BINCODE_SERIALIZER_ID,
            require_cookie: None,
            watch_heartbeat_interval: Duration::from_secs(1),
            watch_failure_threshold: 5,
            phi_threshold: 8.0,
            phi_max_sample_size: 1000,
            phi_min_std_deviation: Duration::from_millis(100),
            phi_acceptable_heartbeat_pause: Duration::from_secs(3),
            tls: crate::tls::TlsConfig::default(),
            maximum_payload_size: 256 * 1024,
        }
    }
}

impl RemoteSettings {
    /// Read overrides from the given config. Any missing key falls back to
    /// the default value. Layout mirrors Akka.NET's `akka.remote.dot-netty.tcp.*`.
    pub fn from_config(_cfg: &Config) -> Self {
        // The rakka-config crate's reader is intentionally minimal at this
        // stage. Future versions will pull `akka.remote.*` keys here.
        Self::default()
    }

    pub fn with_hostname(mut self, host: impl Into<String>) -> Self {
        self.hostname = Some(host.into());
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_protocol(mut self, p: impl Into<String>) -> Self {
        self.protocol = p.into();
        self
    }

    /// Override the phi-accrual threshold (default 8.0).
    pub fn with_phi_threshold(mut self, t: f64) -> Self {
        self.phi_threshold = t;
        self
    }

    /// Override the phi-accrual sample size (default 1000).
    pub fn with_phi_sample_size(mut self, n: usize) -> Self {
        self.phi_max_sample_size = n;
        self
    }

    /// Override the std-dev floor (default 100ms).
    pub fn with_phi_min_std_deviation(mut self, d: Duration) -> Self {
        self.phi_min_std_deviation = d;
        self
    }

    /// Override the acceptable heart-beat pause (default 3s).
    pub fn with_phi_acceptable_pause(mut self, d: Duration) -> Self {
        self.phi_acceptable_heartbeat_pause = d;
        self
    }

    /// Override the TLS configuration (default: disabled).
    pub fn with_tls(mut self, t: crate::tls::TlsConfig) -> Self {
        self.tls = t;
        self
    }

    /// Override the chunking threshold (default 256 KiB).
    pub fn with_maximum_payload_size(mut self, n: usize) -> Self {
        self.maximum_payload_size = n;
        self
    }
}
