//! Akka protocol data units. akka.net: `Remote/Transport/AkkaPduCodec.cs`.
//!
//! Every byte that crosses an association is one of these PDUs:
//!
//! * `Associate`  — handshake, initiated by the connecting side.
//! * `Disassociate` — graceful or quarantine teardown.
//! * `Heartbeat` — liveness ping when the writer is otherwise idle.
//! * `Payload` — a user / system `RemoteEnvelope`.
//! * `Ack` — sliding-window acknowledgement.
//!
//! Wire format: bincode-serialized `AkkaPdu` framed by the transport
//! (length-prefix u32 big-endian).

use serde::{Deserialize, Serialize};

use rakka_core::actor::Address;

use crate::envelope::RemoteEnvelope;

/// One frame on the wire.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AkkaPdu {
    Associate(AssociateInfo),
    Disassociate(DisassociateReason),
    Heartbeat,
    Payload(RemoteEnvelope),
    Ack(AckInfo),
}

/// Carried in the initial `Associate` PDU. The receiving side validates
/// the cookie (if any) and uses `origin` + `uid` to identify the peer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssociateInfo {
    pub origin: Address,
    pub uid: u64,
    pub cookie: Option<String>,
    pub protocol_version: u32,
}

/// Why a peer is disassociating. `Quarantined` is permanent until the
/// quarantine window expires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DisassociateReason {
    /// Normal shutdown — the peer is cooperating.
    Normal,
    /// The peer rejected our handshake (bad cookie, mismatched protocol).
    HandshakeFailure(String),
    /// We detected a UID change and are quarantining the old incarnation.
    Quarantined,
    /// Catch-all error.
    Other(String),
}

/// Sliding-window ack. `cumulative_ack` is the highest `seq_no` we have
/// successfully delivered; `nacks` is the set of explicitly missing seq
/// numbers below that watermark which the sender should resend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckInfo {
    pub cumulative_ack: u64,
    pub nacks: Vec<u64>,
}

/// Wire protocol version. Bump only on backward-incompatible changes.
pub const PROTOCOL_VERSION: u32 = 1;
