//! Row shapes stored in the journal / snapshot tables. Payload bytes are
//! base64-encoded because Table Storage doesn't support raw binary
//! natively, only "typed" properties.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rustakka_persistence::{PersistentRepr, SnapshotMetadata};
use serde::{Deserialize, Serialize};

fn row_key(sequence_nr: u64) -> String {
    format!("{:020}", sequence_nr)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventEntity {
    pub partition_key: String,
    pub row_key: String,
    pub sequence_nr: i64,
    pub payload_b64: String,
    pub manifest: String,
    pub writer_uuid: String,
    pub deleted: bool,
    pub tags_csv: String,
}

impl EventEntity {
    pub fn from_repr(repr: &PersistentRepr) -> Self {
        Self {
            partition_key: repr.persistence_id.clone(),
            row_key: row_key(repr.sequence_nr),
            sequence_nr: repr.sequence_nr as i64,
            payload_b64: B64.encode(&repr.payload),
            manifest: repr.manifest.clone(),
            writer_uuid: repr.writer_uuid.clone(),
            deleted: repr.deleted,
            tags_csv: repr.tags.join(","),
        }
    }

    pub fn into_repr(self) -> PersistentRepr {
        PersistentRepr {
            persistence_id: self.partition_key,
            sequence_nr: self.sequence_nr as u64,
            payload: B64.decode(self.payload_b64).unwrap_or_default(),
            manifest: self.manifest,
            writer_uuid: self.writer_uuid,
            deleted: self.deleted,
            tags: if self.tags_csv.is_empty() {
                Vec::new()
            } else {
                self.tags_csv.split(',').map(|s| s.to_string()).collect()
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SnapshotEntity {
    pub partition_key: String,
    pub row_key: String,
    pub sequence_nr: i64,
    pub payload_b64: String,
    pub timestamp_ms: i64,
}

impl SnapshotEntity {
    pub fn from_meta(meta: &SnapshotMetadata, payload: &[u8]) -> Self {
        Self {
            partition_key: meta.persistence_id.clone(),
            row_key: row_key(meta.sequence_nr),
            sequence_nr: meta.sequence_nr as i64,
            payload_b64: B64.encode(payload),
            timestamp_ms: meta.timestamp as i64,
        }
    }

    pub fn into_parts(self) -> (SnapshotMetadata, Vec<u8>) {
        (
            SnapshotMetadata {
                persistence_id: self.partition_key,
                sequence_nr: self.sequence_nr as u64,
                timestamp: self.timestamp_ms as u64,
            },
            B64.decode(self.payload_b64).unwrap_or_default(),
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
            sequence_nr: 5,
            payload: vec![1, 2, 3],
            manifest: "m".into(),
            writer_uuid: "w".into(),
            deleted: false,
            tags: vec!["a".into(), "b".into()],
        };
        let entity = EventEntity::from_repr(&original);
        let back = entity.into_repr();
        assert_eq!(back.payload, original.payload);
        assert_eq!(back.tags, original.tags);
    }

    #[test]
    fn row_key_is_zero_padded() {
        let entity = EventEntity::from_repr(&PersistentRepr {
            persistence_id: "p".into(),
            sequence_nr: 7,
            payload: vec![],
            manifest: "".into(),
            writer_uuid: "".into(),
            deleted: false,
            tags: vec![],
        });
        assert_eq!(entity.row_key, "00000000000000000007");
    }
}
