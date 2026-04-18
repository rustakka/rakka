//! Shared report schema.
//!
//! The same structure is produced by the Python profiler so the two
//! runtimes can be merged into a single comparison table.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Which workload was measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// Fire-and-forget `tell` into a null actor.
    Tell,
    /// Sequential `ask` round-trips (measures latency).
    Ask,
    /// Spawn many actors and hit each one once (actor-creation cost).
    Fanout,
    /// CPU-bound handler (xxHash-lite compute loop).
    Cpu,
}

impl Scenario {
    pub fn name(self) -> &'static str {
        match self {
            Scenario::Tell => "tell",
            Scenario::Ask => "ask",
            Scenario::Fanout => "fanout",
            Scenario::Cpu => "cpu",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "tell" => Some(Scenario::Tell),
            "ask" => Some(Scenario::Ask),
            "fanout" => Some(Scenario::Fanout),
            "cpu" => Some(Scenario::Cpu),
            _ => None,
        }
    }

    pub fn all() -> &'static [Scenario] {
        &[Scenario::Tell, Scenario::Ask, Scenario::Fanout, Scenario::Cpu]
    }
}

/// One scenario's measurement. Times are in nanoseconds for precision;
/// helpers render them as human-friendly units.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    pub runtime: String,          // "rust" or "python"
    pub scenario: Scenario,
    pub config: String,           // free-form (dispatcher, pool size, ...)
    pub messages: u64,            // total messages (or actors for fanout)
    pub elapsed_ns: u64,
    pub throughput_msgs_per_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_delta_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_delta_ns: Option<u64>,
}

impl Measurement {
    pub fn from_throughput(
        runtime: &str,
        scenario: Scenario,
        config: &str,
        messages: u64,
        elapsed: Duration,
    ) -> Self {
        let elapsed_ns = elapsed.as_nanos() as u64;
        let throughput = if elapsed_ns == 0 {
            0.0
        } else {
            (messages as f64) * 1.0e9 / (elapsed_ns as f64)
        };
        Self {
            runtime: runtime.to_string(),
            scenario,
            config: config.to_string(),
            messages,
            elapsed_ns,
            throughput_msgs_per_sec: throughput,
            p50_ns: None,
            p95_ns: None,
            p99_ns: None,
            rss_delta_bytes: None,
            peak_rss_bytes: None,
            cpu_delta_ns: None,
        }
    }

    pub fn with_latencies(mut self, sorted: &[Duration]) -> Self {
        use crate::metrics::percentile;
        self.p50_ns = percentile(sorted, 50.0).map(|d| d.as_nanos() as u64);
        self.p95_ns = percentile(sorted, 95.0).map(|d| d.as_nanos() as u64);
        self.p99_ns = percentile(sorted, 99.0).map(|d| d.as_nanos() as u64);
        self
    }

    pub fn with_memory(mut self, delta: Option<i64>, peak: Option<u64>) -> Self {
        self.rss_delta_bytes = delta;
        self.peak_rss_bytes = peak;
        self
    }

    pub fn with_cpu(mut self, cpu: Option<Duration>) -> Self {
        self.cpu_delta_ns = cpu.map(|d| d.as_nanos() as u64);
        self
    }
}

/// Top-level report — a list of measurements plus some environment metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfilerReport {
    pub runtime: String,
    pub version: String,
    pub host: String,
    pub measurements: Vec<Measurement>,
}

impl ProfilerReport {
    pub fn new(runtime: &str) -> Self {
        Self {
            runtime: runtime.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            host: host_tag(),
            measurements: Vec::new(),
        }
    }

    pub fn push(&mut self, m: Measurement) {
        self.measurements.push(m);
    }

    /// Render as a human-friendly markdown table.
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "# rustakka profiler — {} ({})\n\nhost: `{}`\n\n",
            self.runtime, self.version, self.host
        ));
        out.push_str(
            "| scenario | config | msgs | elapsed | throughput | p50 | p95 | p99 | ΔRSS | CPU |\n",
        );
        out.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
        for m in &self.measurements {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                m.scenario.name(),
                m.config,
                m.messages,
                fmt_ns(m.elapsed_ns),
                fmt_rate(m.throughput_msgs_per_sec),
                fmt_opt_ns(m.p50_ns),
                fmt_opt_ns(m.p95_ns),
                fmt_opt_ns(m.p99_ns),
                fmt_opt_delta(m.rss_delta_bytes),
                fmt_opt_ns(m.cpu_delta_ns),
            ));
        }
        out
    }
}

fn host_tag() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    format!("{os}/{arch} cpus={cpus}")
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.2}µs", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

fn fmt_opt_ns(v: Option<u64>) -> String {
    match v {
        Some(n) => fmt_ns(n),
        None => "n/a".to_string(),
    }
}

fn fmt_rate(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.2}M/s", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.2}k/s", v / 1e3)
    } else {
        format!("{v:.2}/s")
    }
}

fn fmt_opt_delta(v: Option<i64>) -> String {
    match v {
        Some(n) => {
            let abs = n.unsigned_abs();
            let pretty = if abs >= 1 << 30 {
                format!("{:.2}GiB", abs as f64 / (1u64 << 30) as f64)
            } else if abs >= 1 << 20 {
                format!("{:.2}MiB", abs as f64 / (1u64 << 20) as f64)
            } else if abs >= 1 << 10 {
                format!("{:.2}KiB", abs as f64 / (1u64 << 10) as f64)
            } else {
                format!("{abs}B")
            };
            if n < 0 { format!("-{pretty}") } else { format!("+{pretty}") }
        }
        None => "n/a".to_string(),
    }
}
