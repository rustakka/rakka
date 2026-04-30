//! JSON payload shapes stored alongside the sequence number score.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rakka_persistence::{PersistentRepr, SnapshotMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRepr {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub payload_b64: String,
    pub manifest: String,
    pub writer_uuid: String,
    pub deleted: bool,
    pub tags: Vec<String>,
}

impl From<&PersistentRepr> for StoredRepr {
    fn from(r: &PersistentRepr) -> Self {
        Self {
            persistence_id: r.persistence_id.clone(),
            sequence_nr: r.sequence_nr,
            payload_b64: B64.encode(&r.payload),
            manifest: r.manifest.clone(),
            writer_uuid: r.writer_uuid.clone(),
            deleted: r.deleted,
            tags: r.tags.clone(),
        }
    }
}

impl StoredRepr {
    pub fn into_repr(self) -> PersistentRepr {
        PersistentRepr {
            persistence_id: self.persistence_id,
            sequence_nr: self.sequence_nr,
            payload: B64.decode(self.payload_b64).unwrap_or_default(),
            manifest: self.manifest,
            writer_uuid: self.writer_uuid,
            deleted: self.deleted,
            tags: self.tags,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSnapshot {
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub timestamp: u64,
    pub payload_b64: String,
}

impl StoredSnapshot {
    pub fn new(meta: &SnapshotMetadata, payload: &[u8]) -> Self {
        Self {
            persistence_id: meta.persistence_id.clone(),
            sequence_nr: meta.sequence_nr,
            timestamp: meta.timestamp,
            payload_b64: B64.encode(payload),
        }
    }

    pub fn into_parts(self) -> (SnapshotMetadata, Vec<u8>) {
        let payload = B64.decode(self.payload_b64).unwrap_or_default();
        (
            SnapshotMetadata {
                persistence_id: self.persistence_id,
                sequence_nr: self.sequence_nr,
                timestamp: self.timestamp,
            },
            payload,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_repr() {
        let original = PersistentRepr {
            persistence_id: "p1".into(),
            sequence_nr: 7,
            payload: vec![0, 1, 2, 255],
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: vec!["t1".into()],
        };
        let stored = StoredRepr::from(&original);
        let back = stored.into_repr();
        assert_eq!(back.persistence_id, original.persistence_id);
        assert_eq!(back.sequence_nr, original.sequence_nr);
        assert_eq!(back.payload, original.payload);
        assert_eq!(back.tags, original.tags);
    }

    #[test]
    fn round_trip_snapshot() {
        let meta = SnapshotMetadata { persistence_id: "p".into(), sequence_nr: 9, timestamp: 100 };
        let payload = b"state".to_vec();
        let stored = StoredSnapshot::new(&meta, &payload);
        let (m, p) = stored.into_parts();
        assert_eq!(m.sequence_nr, 9);
        assert_eq!(p, payload);
    }
}
