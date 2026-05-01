//! TLS settings + helpers.
//!
//! Phase 5.E of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Remote.SslSettings`. The actual handshake plugs into the
//! `TcpTransport` through the `TlsConfig` shape; that wiring lands
//! once the reader/writer split (5.D) is in.
//!
//! For now we ship the typed configuration + a lightweight helper
//! to load PEM-encoded cert/key pairs into the form `rustls`
//! expects. Both feature-gated bits are deferred to keep the slim
//! build dep-free.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TlsError {
    #[error("io error reading `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid PEM input: {0}")]
    Pem(String),
}

/// TLS configuration knobs surfaced on `RemoteSettings`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TlsConfig {
    /// PEM-encoded certificate chain.
    pub cert_path: Option<PathBuf>,
    /// PEM-encoded private key.
    pub key_path: Option<PathBuf>,
    /// PEM-encoded trust roots (defaults to the OS root store).
    pub ca_path: Option<PathBuf>,
    /// Require client cert verification (mTLS).
    pub require_client_auth: bool,
    /// SNI hostname when initiating outbound connections.
    pub server_name: Option<String>,
    /// Allow unknown roots (dev / self-signed). MUST be `false` in
    /// production.
    pub insecure_accept_any_cert: bool,
}

impl TlsConfig {
    pub fn enabled(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    pub fn with_cert(mut self, p: impl Into<PathBuf>) -> Self {
        self.cert_path = Some(p.into());
        self
    }

    pub fn with_key(mut self, p: impl Into<PathBuf>) -> Self {
        self.key_path = Some(p.into());
        self
    }

    pub fn with_ca(mut self, p: impl Into<PathBuf>) -> Self {
        self.ca_path = Some(p.into());
        self
    }

    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    pub fn with_client_auth(mut self, on: bool) -> Self {
        self.require_client_auth = on;
        self
    }
}

/// Best-effort PEM block extraction. Returns the DER bytes of every
/// block whose header matches `expected_label` (e.g. "CERTIFICATE",
/// "PRIVATE KEY"). Used as a base for the eventual `rustls`
/// integration without taking the dep here.
pub fn parse_pem_blocks(text: &str, expected_label: &str) -> Result<Vec<Vec<u8>>, TlsError> {
    let begin = format!("-----BEGIN {expected_label}-----");
    let end = format!("-----END {expected_label}-----");
    let mut out = Vec::new();
    let mut iter = text.split(&begin[..]);
    let _ = iter.next(); // discard preamble
    for block in iter {
        let Some(end_idx) = block.find(&end[..]) else {
            return Err(TlsError::Pem(format!("missing {end}")));
        };
        let body: String = block[..end_idx]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let bytes = base64_decode(&body)
            .map_err(|e| TlsError::Pem(format!("base64: {e}")))?;
        out.push(bytes);
    }
    Ok(out)
}

/// Tiny standard-base64 decoder (no `=` padding required). Returns
/// the decoded bytes or a string describing the offending character.
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Option<u8> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    }
    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=' && !b.is_ascii_whitespace()).collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for (i, &b) in bytes.iter().enumerate() {
        let v = val(b).ok_or_else(|| format!("bad char at {i}: {b:#x}"))?;
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_requires_both_cert_and_key() {
        let mut t = TlsConfig::default();
        assert!(!t.enabled());
        t = t.with_cert("/etc/cert.pem");
        assert!(!t.enabled());
        t = t.with_key("/etc/key.pem");
        assert!(t.enabled());
    }

    #[test]
    fn builders_chain() {
        let t = TlsConfig::default()
            .with_cert("/c")
            .with_key("/k")
            .with_ca("/ca")
            .with_server_name("example.com")
            .with_client_auth(true);
        assert!(t.enabled());
        assert_eq!(t.server_name.as_deref(), Some("example.com"));
        assert!(t.require_client_auth);
    }

    #[test]
    fn parse_pem_extracts_certificate_block() {
        let pem = "\
-----BEGIN CERTIFICATE-----
SGVsbG8gd29ybGQh
-----END CERTIFICATE-----
";
        let blocks = parse_pem_blocks(pem, "CERTIFICATE").unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0], b"Hello world!");
    }

    #[test]
    fn parse_pem_handles_multiple_blocks() {
        let pem = "\
-----BEGIN CERTIFICATE-----
SGVsbG8=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
V29ybGQ=
-----END CERTIFICATE-----
";
        let blocks = parse_pem_blocks(pem, "CERTIFICATE").unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0], b"Hello");
        assert_eq!(blocks[1], b"World");
    }

    #[test]
    fn parse_pem_missing_end_errors() {
        let pem = "-----BEGIN CERTIFICATE-----\nSGV=\n";
        let r = parse_pem_blocks(pem, "CERTIFICATE");
        assert!(matches!(r, Err(TlsError::Pem(_))));
    }
}
