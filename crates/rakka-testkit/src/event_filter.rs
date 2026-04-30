//! `EventFilter` — observes events on an `EventStream` and blocks until
//! expected number of matches are seen. akka.net: `Akka.TestKit/EventFilter/`.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::event::{EventStream, Subscription};

pub struct EventFilter {
    matches: Arc<AtomicUsize>,
    _sub: Subscription,
}

impl EventFilter {
    pub fn new<T: Any + Send + Sync + 'static, F>(stream: &EventStream, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let matches = Arc::new(AtomicUsize::new(0));
        let c = matches.clone();
        let sub = stream.subscribe(move |v: &T| {
            if predicate(v) {
                c.fetch_add(1, Ordering::Relaxed);
            }
        });
        Self { matches, _sub: sub }
    }

    pub fn count(&self) -> usize {
        self.matches.load(Ordering::Relaxed)
    }

    pub async fn await_count(&self, n: usize, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.count() >= n {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn filter_counts_matches() {
        let bus = EventStream::new();
        let f = EventFilter::new::<u32, _>(&bus, |v| *v > 5);
        bus.publish(1u32);
        bus.publish(10u32);
        bus.publish(7u32);
        assert!(f.await_count(2, Duration::from_millis(100)).await);
    }
}
