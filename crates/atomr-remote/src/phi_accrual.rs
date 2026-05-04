//! Phi accrual failure detector. Straight port of the math from
//! akka.net's `Remote/PhiAccrualFailureDetector.cs`, which itself ports
//! Hayashibara's algorithm.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::failure_detector::FailureDetector;

pub struct PhiAccrualFailureDetector {
    threshold: f64,
    max_samples: usize,
    min_std_deviation: Duration,
    acceptable_heartbeat_pause: Duration,
    first_heartbeat_estimate: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    history: VecDeque<f64>, // intervals in ms
    last_heartbeat: Option<Instant>,
}

impl PhiAccrualFailureDetector {
    pub fn new(
        threshold: f64,
        max_samples: usize,
        min_std_deviation: Duration,
        acceptable_heartbeat_pause: Duration,
        first_heartbeat_estimate: Duration,
    ) -> Self {
        Self {
            threshold,
            max_samples,
            min_std_deviation,
            acceptable_heartbeat_pause,
            first_heartbeat_estimate,
            inner: Mutex::new(Inner { history: VecDeque::new(), last_heartbeat: None }),
        }
    }

    pub fn phi(&self) -> f64 {
        let i = self.inner.lock();
        let Some(last) = i.last_heartbeat else { return 0.0 };
        let time_diff_ms = last.elapsed().as_millis() as f64;
        let (mean, std_dev) = if i.history.is_empty() {
            let m = self.first_heartbeat_estimate.as_millis() as f64;
            (m, (m * 0.25).max(self.min_std_deviation.as_millis() as f64))
        } else {
            let n = i.history.len() as f64;
            let mean = i.history.iter().sum::<f64>() / n;
            let var = i.history.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
            (mean, var.sqrt().max(self.min_std_deviation.as_millis() as f64))
        };
        let adjusted = time_diff_ms - mean - self.acceptable_heartbeat_pause.as_millis() as f64;
        let y = adjusted / std_dev;
        let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
        if adjusted > 0.0 {
            -(e / (1.0 + e)).log10()
        } else {
            -(1.0 - 1.0 / (1.0 + e)).log10()
        }
    }
}

impl FailureDetector for PhiAccrualFailureDetector {
    fn is_available(&self) -> bool {
        self.phi() < self.threshold
    }

    fn is_monitoring(&self) -> bool {
        self.inner.lock().last_heartbeat.is_some()
    }

    fn heartbeat(&self) {
        let now = Instant::now();
        let mut i = self.inner.lock();
        if let Some(prev) = i.last_heartbeat {
            let diff = now.duration_since(prev).as_millis() as f64;
            i.history.push_back(diff);
            if i.history.len() > self.max_samples {
                i.history.pop_front();
            }
        }
        i.last_heartbeat = Some(now);
    }

    fn reset(&self) {
        let mut i = self.inner.lock();
        i.history.clear();
        i.last_heartbeat = None;
    }

    fn since_last_heartbeat(&self) -> Option<Duration> {
        self.inner.lock().last_heartbeat.map(|t| t.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_after_recent_heartbeats() {
        let d = PhiAccrualFailureDetector::new(
            8.0,
            100,
            Duration::from_millis(100),
            Duration::from_secs(3),
            Duration::from_secs(1),
        );
        for _ in 0..5 {
            d.heartbeat();
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(d.is_available());
        assert!(d.is_monitoring());
    }
}
