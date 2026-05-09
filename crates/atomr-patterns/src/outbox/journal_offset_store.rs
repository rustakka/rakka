//! [`JournalOffsetStore`] — durable [`OutboxOffsetStore`] that
//! piggy-backs on any [`atomr_persistence::Journal`] backend.
//!
//! Each `(outbox_name, source_pid)` pair is encoded as a dedicated
//! persistence id (`outbox::<outbox_name>::offsets`). The full
//! offset map is serialized as a single payload on every `save()`
//! and restored by replaying the highest-sequence record on
//! `load()`.
//!
//! Why one bucket per outbox instead of per source-pid: outbox
//! progress is a single coherent cursor; loading it on restart should
//! be a single round trip. Saves are `O(payload-size)` not `O(n
//! sources)`, which matters when an outbox follows thousands of
//! aggregate streams.

use std::collections::HashMap;
use std::sync::Arc;

use atomr_persistence::{Journal, PersistentRepr};
use parking_lot::Mutex;
use tokio::runtime::Handle;

use crate::outbox::OutboxOffsetStore;

/// Durable offset store backed by any `Journal` backend.
///
/// Plug it into [`super::OutboxBuilder::offset_store`] when you want
/// outbox progress to survive process restarts. Pick the same backend
/// you use for aggregate journals to keep the operational surface
/// small (one connection pool, one schema migration).
pub struct JournalOffsetStore<J: Journal> {
    journal: Arc<J>,
    pid: String,
    cache: Mutex<HashMap<String, u64>>,
    writer_uuid: String,
}

impl<J: Journal> JournalOffsetStore<J> {
    /// Construct against `journal`, scoping offsets under
    /// `outbox::<outbox_name>::offsets`. Eagerly hydrates the cache
    /// from the journal — call from an async context.
    pub async fn new(journal: Arc<J>, outbox_name: impl Into<String>) -> Self {
        let outbox_name = outbox_name.into();
        let pid = format!("outbox::{}::offsets", outbox_name);
        let cache = match journal.highest_sequence_nr(&pid, 0).await {
            Ok(highest) if highest > 0 => match journal.replay_messages(&pid, highest, highest, 1).await {
                Ok(reprs) => reprs
                    .into_iter()
                    .last()
                    .filter(|r| !r.deleted)
                    .and_then(|r| decode(&r.payload))
                    .unwrap_or_default(),
                Err(_) => HashMap::new(),
            },
            _ => HashMap::new(),
        };
        Self { journal, pid, cache: Mutex::new(cache), writer_uuid: format!("outbox-{}", rand_id()) }
    }
}

impl<J: Journal> OutboxOffsetStore for JournalOffsetStore<J> {
    fn load(&self) -> HashMap<String, u64> {
        self.cache.lock().clone()
    }

    fn save(&self, offsets: &HashMap<String, u64>) {
        // Update cache, then persist. The cache is the source of
        // truth for the publisher loop's *current* offsets; the
        // journal write makes the cache durable across process
        // restarts.
        let mut merged = {
            let mut guard = self.cache.lock();
            for (k, v) in offsets {
                guard.insert(k.clone(), *v);
            }
            guard.clone()
        };
        // Drop nothing — keep merged as the full snapshot to write.
        let payload = encode(&merged);
        merged.clear();
        let _ = merged;

        let journal = self.journal.clone();
        let pid = self.pid.clone();
        let writer_uuid = self.writer_uuid.clone();
        // Fire-and-forget the async write. `OutboxOffsetStore::save`
        // is sync, but Journal::write_messages is async — we hop
        // onto the current tokio runtime if one is running.
        let task = async move {
            let next_seq = journal.highest_sequence_nr(&pid, 0).await.unwrap_or(0) + 1;
            let _ = journal
                .write_messages(vec![PersistentRepr {
                    persistence_id: pid,
                    sequence_nr: next_seq,
                    payload,
                    manifest: "outbox-offsets".into(),
                    writer_uuid,
                    deleted: false,
                    tags: vec!["outbox-offsets".into()],
                }])
                .await;
        };
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(task);
        } else {
            // No tokio runtime — best we can do is drop the write.
            // This path is intended for debug/test environments only.
            tracing::warn!(
                "JournalOffsetStore::save called outside a tokio runtime; offset not durably written"
            );
            std::mem::drop(task);
        }
    }
}

fn encode(map: &HashMap<String, u64>) -> Vec<u8> {
    // Simple framed: [u32 count][u32 key_len][key bytes][u64 value]…
    let mut out = Vec::with_capacity(4 + map.len() * 24);
    out.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for (k, v) in map {
        let kb = k.as_bytes();
        out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        out.extend_from_slice(kb);
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn decode(bytes: &[u8]) -> Option<HashMap<String, u64>> {
    if bytes.len() < 4 {
        return None;
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().ok()?) as usize;
    let mut p = 4usize;
    let mut out = HashMap::with_capacity(count);
    for _ in 0..count {
        if bytes.len() < p + 4 {
            return None;
        }
        let kl = u32::from_le_bytes(bytes[p..p + 4].try_into().ok()?) as usize;
        p += 4;
        if bytes.len() < p + kl + 8 {
            return None;
        }
        let key = std::str::from_utf8(&bytes[p..p + kl]).ok()?.to_string();
        p += kl;
        let v = u64::from_le_bytes(bytes[p..p + 8].try_into().ok()?);
        p += 8;
        out.insert(key, v);
    }
    Some(out)
}

fn rand_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{nanos:x}")
}
