//! Message chunking for payloads that exceed `maximum-frame-size`.
//!
//! Phase 5.F of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Remote.Configuration.Maximum-Frame-Size` + per-PDU
//! length-prefix split. Senders that produce payloads larger than
//! `chunk_size` use [`Chunker::split`] to fragment into ordered
//! chunks; receivers feed each chunk to [`Reassembler::push`] until
//! `Some(Vec<u8>)` comes back.
//!
//! The wire envelope around chunks is a tiny `(message_id, chunk_idx,
//! total_chunks, payload)` tuple. The remote codec wraps chunks in
//! `AkkaPdu::Payload` like any other message.

use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ChunkError {
    #[error("invalid chunk: idx={idx} >= total={total}")]
    InvalidIndex { idx: u32, total: u32 },
    #[error("size mismatch for message {message_id}: previously {previous}, now {now}")]
    SizeMismatch { message_id: u64, previous: u32, now: u32 },
}

/// One fragment produced by [`Chunker::split`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub message_id: u64,
    pub chunk_idx: u32,
    pub total_chunks: u32,
    pub payload: Vec<u8>,
}

impl Chunk {
    /// Convenience: serialize to a `(header, payload)` pair so the
    /// remote codec can frame them on the wire. Header is 16 bytes:
    /// `[message_id u64 le][chunk_idx u32 le][total u32 le]`.
    pub fn to_wire(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.payload.len());
        buf.extend_from_slice(&self.message_id.to_le_bytes());
        buf.extend_from_slice(&self.chunk_idx.to_le_bytes());
        buf.extend_from_slice(&self.total_chunks.to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&bytes[..8]);
        let mut idx_bytes = [0u8; 4];
        idx_bytes.copy_from_slice(&bytes[8..12]);
        let mut total_bytes = [0u8; 4];
        total_bytes.copy_from_slice(&bytes[12..16]);
        Some(Self {
            message_id: u64::from_le_bytes(id_bytes),
            chunk_idx: u32::from_le_bytes(idx_bytes),
            total_chunks: u32::from_le_bytes(total_bytes),
            payload: bytes[16..].to_vec(),
        })
    }
}

/// Sender-side: split large payloads into ordered fragments.
pub struct Chunker {
    pub chunk_size: usize,
}

impl Chunker {
    pub fn new(chunk_size: usize) -> Self {
        assert!(chunk_size >= 1, "chunk_size must be >= 1");
        Self { chunk_size }
    }

    /// Split `payload` into fragments. Each fragment shares the same
    /// `message_id`. Returns at least one chunk even for an empty
    /// payload (`total_chunks = 1`, empty payload).
    pub fn split(&self, message_id: u64, payload: &[u8]) -> Vec<Chunk> {
        if payload.is_empty() {
            return vec![Chunk { message_id, chunk_idx: 0, total_chunks: 1, payload: Vec::new() }];
        }
        let total = payload.len().div_ceil(self.chunk_size);
        let mut out = Vec::with_capacity(total);
        for (i, chunk_payload) in payload.chunks(self.chunk_size).enumerate() {
            out.push(Chunk {
                message_id,
                chunk_idx: i as u32,
                total_chunks: total as u32,
                payload: chunk_payload.to_vec(),
            });
        }
        out
    }
}

/// Receiver-side: collect chunks for the same `message_id` until the
/// full set arrives, then return the reassembled payload.
#[derive(Default)]
pub struct Reassembler {
    pending: HashMap<u64, Pending>,
}

struct Pending {
    total: u32,
    chunks: Vec<Option<Vec<u8>>>,
    received: u32,
    started_at: std::time::Instant,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one chunk. Returns `Some(payload)` when the message is
    /// complete, `None` while still waiting for siblings.
    pub fn push(&mut self, chunk: Chunk) -> Result<Option<Vec<u8>>, ChunkError> {
        if chunk.total_chunks == 0 || chunk.chunk_idx >= chunk.total_chunks {
            return Err(ChunkError::InvalidIndex { idx: chunk.chunk_idx, total: chunk.total_chunks });
        }
        let entry = self.pending.entry(chunk.message_id).or_insert_with(|| Pending {
            total: chunk.total_chunks,
            chunks: (0..chunk.total_chunks).map(|_| None).collect(),
            received: 0,
            started_at: std::time::Instant::now(),
        });
        if entry.total != chunk.total_chunks {
            return Err(ChunkError::SizeMismatch {
                message_id: chunk.message_id,
                previous: entry.total,
                now: chunk.total_chunks,
            });
        }
        let slot = &mut entry.chunks[chunk.chunk_idx as usize];
        if slot.is_none() {
            *slot = Some(chunk.payload);
            entry.received += 1;
        }
        if entry.received < entry.total {
            return Ok(None);
        }
        // All chunks present — concatenate in order.
        let pending = self.pending.remove(&chunk.message_id).expect("just present");
        let total_len: usize = pending.chunks.iter().filter_map(|c| c.as_ref()).map(|v| v.len()).sum();
        let mut out = Vec::with_capacity(total_len);
        for buf in pending.chunks.into_iter().flatten() {
            out.extend_from_slice(&buf);
        }
        Ok(Some(out))
    }

    pub fn pending_message_count(&self) -> usize {
        self.pending.len()
    }

    /// Discard partial reassemblies older than `older_than`. Returns
    /// the count of entries swept. Call on a low-frequency tick to
    /// keep the reassembler bounded against peers that fall silent
    /// mid-message.
    pub fn gc_expired(&mut self, older_than: std::time::Duration) -> usize {
        let now = std::time::Instant::now();
        let before = self.pending.len();
        self.pending.retain(|_, p| now.duration_since(p.started_at) < older_than);
        before - self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_packs_chunks_with_indices() {
        let c = Chunker::new(3);
        let chunks = c.split(42, b"abcdefgh");
        assert_eq!(chunks.len(), 3);
        assert_eq!(
            chunks[0],
            Chunk { message_id: 42, chunk_idx: 0, total_chunks: 3, payload: b"abc".to_vec() }
        );
        assert_eq!(
            chunks[1],
            Chunk { message_id: 42, chunk_idx: 1, total_chunks: 3, payload: b"def".to_vec() }
        );
        assert_eq!(
            chunks[2],
            Chunk { message_id: 42, chunk_idx: 2, total_chunks: 3, payload: b"gh".to_vec() }
        );
    }

    #[test]
    fn empty_payload_yields_single_chunk() {
        let chunks = Chunker::new(8).split(7, b"");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].payload.is_empty());
        assert_eq!(chunks[0].total_chunks, 1);
    }

    #[test]
    fn round_trip_split_then_reassemble() {
        let c = Chunker::new(4);
        let payload = b"hello world! this is a longer payload than 4 bytes.";
        let chunks = c.split(1, payload);
        let mut r = Reassembler::new();
        let mut completed = None;
        for chunk in chunks {
            if let Some(full) = r.push(chunk).unwrap() {
                completed = Some(full);
            }
        }
        assert_eq!(completed.unwrap(), payload);
        assert_eq!(r.pending_message_count(), 0);
    }

    #[test]
    fn out_of_order_chunks_reassemble_correctly() {
        let c = Chunker::new(2);
        let mut chunks = c.split(7, b"abcdef");
        chunks.reverse(); // hand them to the receiver in reverse order
        let mut r = Reassembler::new();
        let mut full = None;
        for chunk in chunks {
            if let Some(payload) = r.push(chunk).unwrap() {
                full = Some(payload);
            }
        }
        assert_eq!(full.unwrap(), b"abcdef");
    }

    #[test]
    fn duplicate_chunks_are_idempotent() {
        let c = Chunker::new(2);
        let chunks = c.split(9, b"abcd");
        let mut r = Reassembler::new();
        let _ = r.push(chunks[0].clone()).unwrap();
        // Re-push the same chunk; receiver count shouldn't double.
        let _ = r.push(chunks[0].clone()).unwrap();
        let full = r.push(chunks[1].clone()).unwrap();
        assert_eq!(full.unwrap(), b"abcd");
    }

    #[test]
    fn invalid_index_errors() {
        let mut r = Reassembler::new();
        let bad = Chunk { message_id: 1, chunk_idx: 5, total_chunks: 3, payload: vec![] };
        let result = r.push(bad);
        assert!(matches!(result, Err(ChunkError::InvalidIndex { .. })));
    }

    #[test]
    fn size_mismatch_errors() {
        let mut r = Reassembler::new();
        let _ = r.push(Chunk { message_id: 1, chunk_idx: 0, total_chunks: 3, payload: vec![1] });
        let conflicting = Chunk { message_id: 1, chunk_idx: 1, total_chunks: 4, payload: vec![2] };
        assert!(matches!(r.push(conflicting), Err(ChunkError::SizeMismatch { .. })));
    }

    #[test]
    fn wire_round_trip() {
        let c = Chunk { message_id: 0xdeadbeef, chunk_idx: 3, total_chunks: 7, payload: b"hello".to_vec() };
        let bytes = c.to_wire();
        let parsed = Chunk::from_wire(&bytes).unwrap();
        assert_eq!(parsed, c);
    }

    #[test]
    fn gc_expired_evicts_old_partials() {
        let mut r = Reassembler::new();
        // Insert a partial that will be aged out.
        let _ = r.push(Chunk { message_id: 99, chunk_idx: 0, total_chunks: 2, payload: vec![1] });
        assert_eq!(r.pending_message_count(), 1);
        // Sweep with a zero-age threshold: evicts immediately.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let swept = r.gc_expired(std::time::Duration::from_millis(1));
        assert_eq!(swept, 1);
        assert_eq!(r.pending_message_count(), 0);
    }

    #[test]
    fn gc_expired_keeps_fresh_partials() {
        let mut r = Reassembler::new();
        let _ = r.push(Chunk { message_id: 1, chunk_idx: 0, total_chunks: 2, payload: vec![1] });
        let swept = r.gc_expired(std::time::Duration::from_secs(60));
        assert_eq!(swept, 0);
        assert_eq!(r.pending_message_count(), 1);
    }
}
