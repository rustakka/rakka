//! Persistence probe — tracks a roll-up of journal activity and provides
//! an admin trait for listing `persistence_id`s + highest sequence
//! numbers. Default impls are feature-gated so the telemetry crate works
//! even when persistence is disabled.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{JournalInfo, JournalWriteInfo, PersistenceIdStat, PersistenceSnapshot};

/// Admin surface journals can implement to expose their contents to the
/// dashboard. Default methods return empty vectors so implementing it is
/// opt-in per backend.
#[async_trait]
pub trait JournalAdmin: Send + Sync + 'static {
    fn name(&self) -> &str;

    async fn list_persistence_ids(&self) -> Vec<String> {
        Vec::new()
    }

    async fn highest_sequence_nr_for(&self, persistence_id: &str) -> u64 {
        let _ = persistence_id;
        0
    }

    async fn event_count_for(&self, persistence_id: &str) -> u64 {
        let _ = persistence_id;
        0
    }
}

pub struct PersistenceProbe {
    bus: TelemetryBus,
    journals: RwLock<Vec<Arc<dyn JournalAdmin>>>,
    recent_writes: RwLock<std::collections::VecDeque<JournalWriteInfo>>,
    total_events: AtomicU64,
    max_recent: usize,
}

impl PersistenceProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self {
            bus,
            journals: RwLock::new(Vec::new()),
            recent_writes: RwLock::new(std::collections::VecDeque::with_capacity(128)),
            total_events: AtomicU64::new(0),
            max_recent: 128,
        }
    }

    pub fn register_journal(&self, admin: Arc<dyn JournalAdmin>) {
        self.journals.write().push(admin);
    }

    pub fn record_write(&self, journal: &str, persistence_id: &str, sequence_nr: u64) {
        let info = JournalWriteInfo {
            journal: journal.to_string(),
            persistence_id: persistence_id.to_string(),
            sequence_nr,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        self.total_events.fetch_add(1, Ordering::Relaxed);
        {
            let mut b = self.recent_writes.write();
            if b.len() == self.max_recent {
                b.pop_front();
            }
            b.push_back(info.clone());
        }
        self.bus.publish(TelemetryEvent::JournalWrite(info));
    }

    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> PersistenceSnapshot {
        PersistenceSnapshot {
            journals: Vec::new(),
            total_events: self.total_events(),
            recent_writes: self.recent_writes.read().iter().cloned().collect(),
        }
    }

    /// Same as [`Self::snapshot`] but populates per-journal persistence
    /// ids + sequence numbers by calling into every registered
    /// [`JournalAdmin`]. Awaits each admin sequentially.
    pub async fn snapshot_async(&self) -> PersistenceSnapshot {
        let journals = self.journals.read().clone();
        let mut out: Vec<JournalInfo> = Vec::with_capacity(journals.len());
        for j in journals {
            let ids = j.list_persistence_ids().await;
            let mut pids: Vec<PersistenceIdStat> = Vec::with_capacity(ids.len());
            for id in ids {
                let seq = j.highest_sequence_nr_for(&id).await;
                let count = j.event_count_for(&id).await;
                pids.push(PersistenceIdStat {
                    persistence_id: id,
                    highest_sequence_nr: seq,
                    event_count: count,
                });
            }
            out.push(JournalInfo { name: j.name().to_string(), persistence_ids: pids });
        }
        PersistenceSnapshot {
            journals: out,
            total_events: self.total_events(),
            recent_writes: self.recent_writes.read().iter().cloned().collect(),
        }
    }
}

/// Admin wrapper around [`atomr_persistence::InMemoryJournal`].
/// Feature-gated.
#[cfg(feature = "persistence")]
pub struct InMemoryJournalAdmin {
    name: String,
    inner: Arc<atomr_persistence::InMemoryJournal>,
}

#[cfg(feature = "persistence")]
impl InMemoryJournalAdmin {
    pub fn new(name: impl Into<String>, journal: Arc<atomr_persistence::InMemoryJournal>) -> Self {
        Self { name: name.into(), inner: journal }
    }
}

#[cfg(feature = "persistence")]
#[async_trait]
impl JournalAdmin for InMemoryJournalAdmin {
    fn name(&self) -> &str {
        &self.name
    }
    async fn list_persistence_ids(&self) -> Vec<String> {
        self.inner.list_persistence_ids()
    }
    async fn highest_sequence_nr_for(&self, pid: &str) -> u64 {
        use atomr_persistence::Journal;
        self.inner.highest_sequence_nr(pid, 0).await.unwrap_or(0)
    }
    async fn event_count_for(&self, pid: &str) -> u64 {
        self.inner.event_count(pid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dummy;
    #[async_trait]
    impl JournalAdmin for Dummy {
        fn name(&self) -> &str {
            "dummy"
        }
        async fn list_persistence_ids(&self) -> Vec<String> {
            vec!["p1".into()]
        }
        async fn highest_sequence_nr_for(&self, _pid: &str) -> u64 {
            42
        }
        async fn event_count_for(&self, _pid: &str) -> u64 {
            3
        }
    }

    #[tokio::test]
    async fn records_writes_and_snapshot_async() {
        let bus = TelemetryBus::new(8);
        let probe = PersistenceProbe::new(bus);
        probe.register_journal(Arc::new(Dummy));
        probe.record_write("j", "p1", 1);
        probe.record_write("j", "p1", 2);
        assert_eq!(probe.total_events(), 2);
        let snap = probe.snapshot_async().await;
        assert_eq!(snap.journals.len(), 1);
        assert_eq!(snap.journals[0].persistence_ids[0].highest_sequence_nr, 42);
    }
}
