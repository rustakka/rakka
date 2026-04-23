//! Streams probe — running-graph counter + list hooked into
//! `ActorMaterializer`.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::bus::{TelemetryBus, TelemetryEvent};
use crate::dto::{StreamGraphInfo, StreamsSnapshot};

pub struct StreamsProbe {
    bus: TelemetryBus,
    active: DashMap<u64, StreamGraphInfo>,
    next_id: AtomicU64,
    started: AtomicU64,
    finished: AtomicU64,
}

impl StreamsProbe {
    pub fn new(bus: TelemetryBus) -> Self {
        Self {
            bus,
            active: DashMap::new(),
            next_id: AtomicU64::new(1),
            started: AtomicU64::new(0),
            finished: AtomicU64::new(0),
        }
    }

    pub fn start_graph(&self, name: impl Into<String>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let name = name.into();
        let info = StreamGraphInfo {
            id,
            name: name.clone(),
            started_at: chrono::Utc::now().to_rfc3339(),
        };
        self.active.insert(id, info);
        self.started.fetch_add(1, Ordering::Relaxed);
        self.bus.publish(TelemetryEvent::StreamsGraphStarted { id, name });
        id
    }

    pub fn finish_graph(&self, id: u64) {
        if self.active.remove(&id).is_some() {
            self.finished.fetch_add(1, Ordering::Relaxed);
            self.bus.publish(TelemetryEvent::StreamsGraphFinished { id });
        }
    }

    pub fn running(&self) -> u64 {
        self.active.len() as u64
    }

    pub fn snapshot(&self) -> StreamsSnapshot {
        StreamsSnapshot {
            running_graphs: self.running(),
            total_started: self.started.load(Ordering::Relaxed),
            total_finished: self.finished.load(Ordering::Relaxed),
            active: self.active.iter().map(|e| e.value().clone()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_running_graphs() {
        let bus = TelemetryBus::new(8);
        let p = StreamsProbe::new(bus);
        let a = p.start_graph("g1");
        let _b = p.start_graph("g2");
        assert_eq!(p.running(), 2);
        p.finish_graph(a);
        assert_eq!(p.running(), 1);
        let s = p.snapshot();
        assert_eq!(s.total_started, 2);
        assert_eq!(s.total_finished, 1);
    }
}
