//! Deadline failure detector. akka.net: `Remote/DeadlineFailureDetector.cs`.
//!
//! A simpler FD: any peer that hasn't heartbeat within `acceptable_heartbeat_pause`
//! is considered unreachable.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::failure_detector::FailureDetector;

pub struct DeadlineFailureDetector {
    pause: Duration,
    last_heartbeat: Mutex<Option<Instant>>,
}

impl DeadlineFailureDetector {
    pub fn new(acceptable_heartbeat_pause: Duration) -> Self {
        Self { pause: acceptable_heartbeat_pause, last_heartbeat: Mutex::new(None) }
    }
}

impl FailureDetector for DeadlineFailureDetector {
    fn is_available(&self) -> bool {
        match *self.last_heartbeat.lock() {
            None => true,
            Some(t) => t.elapsed() < self.pause,
        }
    }

    fn is_monitoring(&self) -> bool {
        self.last_heartbeat.lock().is_some()
    }

    fn heartbeat(&self) {
        *self.last_heartbeat.lock() = Some(Instant::now());
    }

    fn reset(&self) {
        *self.last_heartbeat.lock() = None;
    }

    fn since_last_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.lock().map(|t| t.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_after_pause() {
        let d = DeadlineFailureDetector::new(Duration::from_millis(20));
        d.heartbeat();
        assert!(d.is_available());
        std::thread::sleep(Duration::from_millis(30));
        assert!(!d.is_available());
    }
}
