//! Dead-letter feed — bounded ring buffer + bus publisher. Sits alongside
//! the existing [`rustakka_core::event::DeadLettersSink`]; the sink keeps
//! the original `Any` payloads, while this feed keeps a small serializable
//! summary for the dashboard.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rustakka_core::actor::{ActorPath, DeadLetterObserver};

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::DeadLetterRecord;

pub struct DeadLetterFeed {
    bus: TelemetryBus,
    buf: Mutex<VecDeque<DeadLetterRecord>>,
    capacity: usize,
    total: AtomicU64,
}

impl DeadLetterFeed {
    pub fn new(bus: TelemetryBus, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            bus,
            buf: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            total: AtomicU64::new(0),
        }
    }

    pub fn record(&self, recipient: String, sender: Option<String>, message_type: String, preview: String) {
        let seq = self.total.fetch_add(1, Ordering::Relaxed) + 1;
        let rec = DeadLetterRecord {
            seq,
            recipient,
            sender,
            message_type,
            message_preview: preview,
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        {
            let mut b = self.buf.lock();
            if b.len() == self.capacity {
                b.pop_front();
            }
            b.push_back(rec.clone());
        }
        self.bus.publish(TelemetryEvent::DeadLetter(rec));
    }

    pub fn recent(&self, limit: usize) -> Vec<DeadLetterRecord> {
        let b = self.buf.lock();
        b.iter().rev().take(limit).cloned().collect()
    }

    pub fn total_count(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn buffered(&self) -> usize {
        self.buf.lock().len()
    }
}

impl DeadLetterObserver for DeadLetterFeed {
    fn on_dead_letter(
        &self,
        recipient: &ActorPath,
        sender: Option<&ActorPath>,
        message_type: &'static str,
    ) {
        self.record(
            recipient.to_string(),
            sender.map(|p| p.to_string()),
            message_type.to_string(),
            String::new(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_bounds_ring_buffer() {
        let bus = TelemetryBus::new(8);
        let feed = DeadLetterFeed::new(bus, 3);
        for i in 0..5 {
            feed.record(format!("/user/{i}"), None, "String".into(), "hi".into());
        }
        assert_eq!(feed.total_count(), 5);
        assert_eq!(feed.buffered(), 3);
        let recent = feed.recent(10);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].seq, 5);
    }

    #[tokio::test]
    async fn publishes_on_bus() {
        let bus = TelemetryBus::new(8);
        let mut rx = bus.subscribe();
        let feed = DeadLetterFeed::new(bus, 3);
        feed.record("/user/a".into(), None, "Msg".into(), "preview".into());
        let e = rx.recv().await.unwrap();
        assert_eq!(e.topic(), "dead_letters");
    }
}
