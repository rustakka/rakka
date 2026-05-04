//! BSON-friendly document shapes. `payload` is stored as raw bytes
//! via BSON `Binary`.

use atomr_persistence::{PersistentRepr, SnapshotMetadata};
use mongodb::bson::{spec::BinarySubtype, Binary};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDoc {
    pub persistence_id: String,
    pub sequence_nr: i64,
    pub payload: Binary,
    pub manifest: String,
    pub writer_uuid: String,
    pub deleted: bool,
    pub tags: Vec<String>,
    pub created_at: i64,
}

impl EventDoc {
    pub fn from_repr(r: &PersistentRepr, created_at: i64) -> Self {
        Self {
            persistence_id: r.persistence_id.clone(),
            sequence_nr: r.sequence_nr as i64,
            payload: Binary { subtype: BinarySubtype::Generic, bytes: r.payload.clone() },
            manifest: r.manifest.clone(),
            writer_uuid: r.writer_uuid.clone(),
            deleted: r.deleted,
            tags: r.tags.clone(),
            created_at,
        }
    }

    pub fn into_repr(self) -> PersistentRepr {
        PersistentRepr {
            persistence_id: self.persistence_id,
            sequence_nr: self.sequence_nr as u64,
            payload: self.payload.bytes,
            manifest: self.manifest,
            writer_uuid: self.writer_uuid,
            deleted: self.deleted,
            tags: self.tags,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDoc {
    pub persistence_id: String,
    pub sequence_nr: i64,
    pub payload: Binary,
    pub timestamp: i64,
    pub created_at: i64,
}

impl SnapshotDoc {
    pub fn from_meta(meta: &SnapshotMetadata, payload: Vec<u8>, created_at: i64) -> Self {
        Self {
            persistence_id: meta.persistence_id.clone(),
            sequence_nr: meta.sequence_nr as i64,
            payload: Binary { subtype: BinarySubtype::Generic, bytes: payload },
            timestamp: meta.timestamp as i64,
            created_at,
        }
    }

    pub fn into_parts(self) -> (SnapshotMetadata, Vec<u8>) {
        (
            SnapshotMetadata {
                persistence_id: self.persistence_id,
                sequence_nr: self.sequence_nr as u64,
                timestamp: self.timestamp as u64,
            },
            self.payload.bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip() {
        let original = PersistentRepr {
            persistence_id: "p".into(),
            sequence_nr: 3,
            payload: vec![1, 2, 3],
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: vec!["x".into()],
        };
        let doc = EventDoc::from_repr(&original, 100);
        let back = doc.into_repr();
        assert_eq!(back.payload, original.payload);
        assert_eq!(back.tags, original.tags);
    }
}
