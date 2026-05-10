//! Rolling RTT recorder for the link probe.
//!
//! Tracks the last `WINDOW` round-trip samples in a ring buffer plus
//! lifetime sent / recvd counters. Computes loss% and nearest-rank
//! p50/p95/p99 over the window. No histogram crate dep — for an
//! operator probe the simple sketch is plenty.

use std::time::Duration;

use parking_lot::Mutex;

const WINDOW: usize = 1024;

#[derive(Debug, Default)]
struct Inner {
    sent: u64,
    recvd: u64,
    samples: Vec<Duration>,
    next: usize,
}

#[derive(Debug, Default)]
pub struct Stats {
    inner: Mutex<Inner>,
}

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub sent: u64,
    pub recvd: u64,
    pub loss_pct: f64,
    pub p50: Option<Duration>,
    pub p95: Option<Duration>,
    pub p99: Option<Duration>,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_sent(&self) {
        self.inner.lock().sent += 1;
    }

    pub fn record_recv(&self, rtt: Duration) {
        let mut inner = self.inner.lock();
        inner.recvd += 1;
        if inner.samples.len() < WINDOW {
            inner.samples.push(rtt);
        } else {
            let next = inner.next;
            inner.samples[next] = rtt;
            inner.next = (next + 1) % WINDOW;
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        let loss_pct = if inner.sent == 0 {
            0.0
        } else {
            ((inner.sent - inner.recvd.min(inner.sent)) as f64 / inner.sent as f64) * 100.0
        };
        let mut sorted = inner.samples.clone();
        sorted.sort_unstable();
        Snapshot {
            sent: inner.sent,
            recvd: inner.recvd,
            loss_pct,
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
        }
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    // Nearest-rank: rank = ceil(p * n), 1-indexed.
    let rank = ((p * sorted.len() as f64).ceil() as usize).max(1);
    Some(sorted[rank.min(sorted.len()) - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_is_zero() {
        let s = Stats::new();
        let snap = s.snapshot();
        assert_eq!(snap.sent, 0);
        assert_eq!(snap.recvd, 0);
        assert_eq!(snap.loss_pct, 0.0);
        assert!(snap.p50.is_none());
    }

    #[test]
    fn loss_is_sent_minus_recvd() {
        let s = Stats::new();
        for _ in 0..10 {
            s.record_sent();
        }
        s.record_recv(Duration::from_millis(1));
        s.record_recv(Duration::from_millis(2));
        let snap = s.snapshot();
        assert_eq!(snap.sent, 10);
        assert_eq!(snap.recvd, 2);
        assert!((snap.loss_pct - 80.0).abs() < 1e-9);
    }

    #[test]
    fn percentiles_match_nearest_rank() {
        let s = Stats::new();
        for ms in 1..=100 {
            s.record_sent();
            s.record_recv(Duration::from_millis(ms));
        }
        let snap = s.snapshot();
        // Nearest-rank p50 over 1..=100 is the 50th sample = 50ms.
        assert_eq!(snap.p50, Some(Duration::from_millis(50)));
        assert_eq!(snap.p95, Some(Duration::from_millis(95)));
        assert_eq!(snap.p99, Some(Duration::from_millis(99)));
    }

    #[test]
    fn ring_buffer_overwrites_oldest() {
        let s = Stats::new();
        for ms in 0..(WINDOW as u64 + 100) {
            s.record_sent();
            s.record_recv(Duration::from_millis(ms));
        }
        let snap = s.snapshot();
        assert_eq!(snap.recvd, WINDOW as u64 + 100);
        // Window holds last WINDOW samples (100..WINDOW+100); p99 is near the top.
        let p99 = snap.p99.unwrap().as_millis();
        assert!(p99 > 1000, "expected p99 > 1000ms, got {p99}ms");
    }
}
