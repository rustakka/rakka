//! At-least-once delivery. akka.net: `AtLeastOnceDeliverySemantic`.

use std::collections::BTreeMap;

use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct UnconfirmedDelivery<M> {
    pub delivery_id: u64,
    pub destination: String,
    pub message: M,
}

pub struct AtLeastOnceDelivery<M: Clone + Send + 'static> {
    inner: Mutex<Inner<M>>,
    redeliver_interval_ms: u64,
    warn_after_attempts: u32,
    max_unconfirmed: usize,
}

struct Inner<M: Clone + Send + 'static> {
    next_id: u64,
    unconfirmed: BTreeMap<u64, (UnconfirmedDelivery<M>, u32)>,
}

impl<M: Clone + Send + 'static> AtLeastOnceDelivery<M> {
    pub fn new(redeliver_interval_ms: u64, warn_after: u32, max_unconfirmed: usize) -> Self {
        Self {
            inner: Mutex::new(Inner { next_id: 0, unconfirmed: BTreeMap::new() }),
            redeliver_interval_ms,
            warn_after_attempts: warn_after,
            max_unconfirmed,
        }
    }

    pub fn deliver(&self, destination: impl Into<String>, message: M) -> Option<u64> {
        let mut inner = self.inner.lock();
        if inner.unconfirmed.len() >= self.max_unconfirmed {
            return None;
        }
        inner.next_id += 1;
        let id = inner.next_id;
        inner.unconfirmed.insert(
            id,
            (UnconfirmedDelivery { delivery_id: id, destination: destination.into(), message }, 0),
        );
        Some(id)
    }

    pub fn confirm_delivery(&self, id: u64) -> bool {
        self.inner.lock().unconfirmed.remove(&id).is_some()
    }

    pub fn redeliver(&self) -> Vec<UnconfirmedDelivery<M>> {
        let mut inner = self.inner.lock();
        let mut out = Vec::new();
        for (_, (d, attempts)) in inner.unconfirmed.iter_mut() {
            *attempts += 1;
            out.push(d.clone());
        }
        out
    }

    pub fn warn_threshold(&self) -> u32 {
        self.warn_after_attempts
    }

    pub fn redeliver_interval_ms(&self) -> u64 {
        self.redeliver_interval_ms
    }

    pub fn unconfirmed_count(&self) -> usize {
        self.inner.lock().unconfirmed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirms_remove_from_pending() {
        let alod = AtLeastOnceDelivery::<String>::new(500, 5, 100);
        let id = alod.deliver("dst", "hi".into()).unwrap();
        assert_eq!(alod.unconfirmed_count(), 1);
        assert!(alod.confirm_delivery(id));
        assert_eq!(alod.unconfirmed_count(), 0);
    }

    #[test]
    fn redeliver_yields_unconfirmed() {
        let alod = AtLeastOnceDelivery::<u32>::new(500, 5, 100);
        alod.deliver("a", 1);
        alod.deliver("b", 2);
        assert_eq!(alod.redeliver().len(), 2);
    }
}
