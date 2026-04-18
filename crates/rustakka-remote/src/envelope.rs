//! Remote message envelope. akka.net: `Remote/MessageSerializer.cs` + `Envelope.cs`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteEnvelope {
    pub recipient_path: String,
    pub sender_path: Option<String>,
    pub serializer_id: u32,
    pub manifest: String,
    pub payload: Vec<u8>,
}

impl RemoteEnvelope {
    pub fn new(
        recipient: impl Into<String>,
        sender: Option<String>,
        serializer_id: u32,
        manifest: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            recipient_path: recipient.into(),
            sender_path: sender,
            serializer_id,
            manifest: manifest.into(),
            payload,
        }
    }
}
