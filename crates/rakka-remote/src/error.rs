//! Typed remote-system error.
//!
//! Phase 5 of `docs/full-port-plan.md`. The historical
//! `TransportError::HandshakeRejected(String)` and ad-hoc `Result`
//! shapes get normalized into this single enum. Each variant has a
//! [`RemoteErrorKind`] discriminant suitable for matching and
//! per-kind metrics, plus a free-form message for diagnostics.
//!
//! Existing callers continue to use `TransportError` directly; new
//! code should prefer [`RemoteError`] and let `?` convert. Phase 13
//! removes the legacy `HandshakeRejected(String)` once every
//! call-site has migrated.

use thiserror::Error;

use crate::pdu::AkkaPdu;
use crate::transport::TransportError;

/// Stable discriminant for a [`RemoteError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RemoteErrorKind {
    Handshake,
    Quarantined,
    Tombstoned,
    UnknownPdu,
    Codec,
    Transport,
    Closed,
    Timeout,
    /// Back-pressure: bounded send queue rejected the enqueue (Phase 5.G).
    BackPressure,
    /// Catch-all for less-frequent error sites.
    Other,
}

impl RemoteErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Handshake => "handshake",
            Self::Quarantined => "quarantined",
            Self::Tombstoned => "tombstoned",
            Self::UnknownPdu => "unknown_pdu",
            Self::Codec => "codec",
            Self::Transport => "transport",
            Self::Closed => "closed",
            Self::Timeout => "timeout",
            Self::BackPressure => "back_pressure",
            Self::Other => "other",
        }
    }
}

/// Typed remote-system error.
///
/// Construct via [`RemoteError::new`], or via `From<TransportError>`
/// for transport-layer failures. Matches across `kind` are stable.
#[derive(Debug, Error)]
#[error("{kind:?}: {message}")]
#[non_exhaustive]
pub struct RemoteError {
    pub kind: RemoteErrorKind,
    pub message: String,
}

impl RemoteError {
    pub fn new(kind: RemoteErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    /// Construct an `UnknownPdu` error from a received PDU. Used in
    /// place of `panic!("unexpected pdu …")` so production code never
    /// crashes on a protocol mismatch.
    pub fn unknown_pdu(pdu: &AkkaPdu) -> Self {
        Self::new(RemoteErrorKind::UnknownPdu, format!("unexpected PDU: {pdu:?}"))
    }

    pub fn quarantined(target: impl std::fmt::Display) -> Self {
        Self::new(RemoteErrorKind::Quarantined, format!("{target} is quarantined"))
    }

    pub fn tombstoned(target: impl std::fmt::Display) -> Self {
        Self::new(RemoteErrorKind::Tombstoned, format!("{target} is tombstoned"))
    }
}

impl From<TransportError> for RemoteError {
    fn from(e: TransportError) -> Self {
        let (kind, msg) = match &e {
            TransportError::HandshakeRejected(s) => (RemoteErrorKind::Handshake, s.clone()),
            TransportError::Closed => (RemoteErrorKind::Closed, "transport closed".into()),
            other => (RemoteErrorKind::Transport, other.to_string()),
        };
        Self::new(kind, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_transport_handshake_rejected() {
        let t = TransportError::HandshakeRejected("bad cookie".into());
        let r: RemoteError = t.into();
        assert_eq!(r.kind, RemoteErrorKind::Handshake);
        assert!(r.message.contains("bad cookie"));
    }

    #[test]
    fn quarantined_constructs() {
        let r = RemoteError::quarantined("akka.tcp://Sys@host:7355");
        assert_eq!(r.kind, RemoteErrorKind::Quarantined);
        assert!(r.message.contains("host:7355"));
    }

    #[test]
    fn kind_strings_stable() {
        assert_eq!(RemoteErrorKind::Handshake.as_str(), "handshake");
        assert_eq!(RemoteErrorKind::UnknownPdu.as_str(), "unknown_pdu");
    }
}
