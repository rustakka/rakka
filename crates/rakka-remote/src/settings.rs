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
}
