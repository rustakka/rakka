//! Dead-letter sink.

use std::any::Any;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::actor::ActorPath;

/// Why a message ended up at the dead-letter sink.
/// `Event/AllDeadLetters.cs`, `Event/DroppedMessage.cs`,
/// `Event/SuppressedDeadLetter.cs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeadLetterReason {
    /// Default — no recipient (terminated, unknown, or never existed).
    #[default]
    NoRecipient,
    /// Message was dropped due to mailbox overflow.
    Dropped,
    /// Suppressed by upstream policy (e.g. system messages after stop)
    /// — used to keep the dead-letter log readable.
    Suppressed,
}

/// Filter applied to incoming dead letters. The default filter accepts
/// every reason; tighter filters drop noisy categories (e.g. logging
/// dropped-message bursts only at `tracing::trace`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadLetterFilter {
    pub accept_no_recipient: bool,
    pub accept_dropped: bool,
    pub accept_suppressed: bool,
}

impl Default for DeadLetterFilter {
    fn default() -> Self {
        Self { accept_no_recipient: true, accept_dropped: true, accept_suppressed: false }
    }
}

impl DeadLetterFilter {
    pub fn accepts(&self, reason: DeadLetterReason) -> bool {
        match reason {
            DeadLetterReason::NoRecipient => self.accept_no_recipient,
            DeadLetterReason::Dropped => self.accept_dropped,
            DeadLetterReason::Suppressed => self.accept_suppressed,
        }
    }
}

#[derive(Debug)]
pub struct DeadLetter {
    pub recipient: ActorPath,
    pub sender: Option<ActorPath>,
    pub message: Box<dyn Any + Send>,
    pub reason: DeadLetterReason,
}

#[derive(Clone)]
pub struct DeadLettersSink {
    buf: Arc<Mutex<Vec<DeadLetter>>>,
    filter: Arc<Mutex<DeadLetterFilter>>,
}

impl Default for DeadLettersSink {
    fn default() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
            filter: Arc::new(Mutex::new(DeadLetterFilter::default())),
        }
    }
}

impl DeadLettersSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the active filter. Subsequent `push` calls consult it.
    pub fn set_filter(&self, f: DeadLetterFilter) {
        *self.filter.lock() = f;
    }

    pub fn filter(&self) -> DeadLetterFilter {
        *self.filter.lock()
    }

    pub fn push(&self, dl: DeadLetter) {
        let f = *self.filter.lock();
        if !f.accepts(dl.reason) {
            return;
        }
        tracing::warn!(recipient = %dl.recipient, reason = ?dl.reason, "dead letter");
        self.buf.lock().push(dl);
    }

    pub fn drain(&self) -> Vec<DeadLetter> {
        std::mem::take(&mut *self.buf.lock())
    }

    pub fn len(&self) -> usize {
        self.buf.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Address;

    fn make(reason: DeadLetterReason) -> DeadLetter {
        DeadLetter {
            recipient: ActorPath::root(Address::local("S")).child("x"),
            sender: None,
            message: Box::new(1u32),
            reason,
        }
    }

    #[test]
    fn stores_dead_letters() {
        let s = DeadLettersSink::new();
        s.push(make(DeadLetterReason::NoRecipient));
        assert_eq!(s.len(), 1);
        let d = s.drain();
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn default_filter_drops_suppressed() {
        let s = DeadLettersSink::new();
        s.push(make(DeadLetterReason::Suppressed));
        assert!(s.is_empty(), "default filter should drop Suppressed");
    }

    #[test]
    fn custom_filter_drops_dropped_category() {
        let s = DeadLettersSink::new();
        s.set_filter(DeadLetterFilter {
            accept_no_recipient: true,
            accept_dropped: false,
            accept_suppressed: false,
        });
        s.push(make(DeadLetterReason::Dropped));
        assert!(s.is_empty());
        s.push(make(DeadLetterReason::NoRecipient));
        assert_eq!(s.len(), 1);
    }
}
