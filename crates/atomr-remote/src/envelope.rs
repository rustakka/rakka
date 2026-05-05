//! Remote message envelope.+
//! `Remote/RemoteEnvelope.cs`.

use serde::{Deserialize, Serialize};

/// A `RemoteEnvelope` is the unit of payload delivery — one user message
/// (or system control) bound for one recipient. The envelope itself is
/// always serialized via bincode regardless of which `serializer_id` is
/// used for the inner `payload`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEnvelope {
    /// Full path string of the recipient, including address scheme.
    pub recipient_path: String,
    /// Optional sender path for `tell_with_sender` / ask routing.
    pub sender_path: Option<String>,
    /// UID of the sending `ActorSystem` (lets the receiver detect a peer
    /// restart and discard stale state).
    pub sender_uid: u64,
    /// Recipient's expected `ActorSystem` UID; receiver drops envelopes
    /// whose `recipient_uid` does not match the local UID, which avoids
    /// surprise message delivery to a freshly-restarted system.
    /// `0` means "any UID" (best-effort delivery).
    pub recipient_uid: u64,
    /// Monotonic sequence number assigned by the sending Endpoint. Used
    /// for ack'd delivery and duplicate suppression.
    pub seq_no: u64,
    /// Serializer identifier (looked up in the receiving system's
    /// `SerializerRegistry`).
    pub serializer_id: u32,
    /// Type manifest — usually `std::any::type_name::<M>()`.
    pub manifest: String,
    /// `true` for `RemoteSystemMsg` (Stop/Watch/Unwatch/Terminated),
    /// `false` for user messages.
    pub system: bool,
    /// Serialized payload bytes.
    pub payload: Vec<u8>,
}

impl RemoteEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn user(
        recipient: impl Into<String>,
        sender: Option<String>,
        sender_uid: u64,
        recipient_uid: u64,
        seq_no: u64,
        serializer_id: u32,
        manifest: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            recipient_path: recipient.into(),
            sender_path: sender,
            sender_uid,
            recipient_uid,
            seq_no,
            serializer_id,
            manifest: manifest.into(),
            system: false,
            payload,
        }
    }

    pub fn system_msg(
        recipient: impl Into<String>,
        sender_uid: u64,
        recipient_uid: u64,
        seq_no: u64,
        manifest: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            recipient_path: recipient.into(),
            sender_path: None,
            sender_uid,
            recipient_uid,
            seq_no,
            serializer_id: crate::serialization::SYSTEM_SERIALIZER_ID,
            manifest: manifest.into(),
            system: true,
            payload,
        }
    }
}
