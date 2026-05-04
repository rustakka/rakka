//! Dead-letter sink. akka.net: `Actor/DeadLetterMailbox.cs`, `Event/*DeadLetter.cs`.

use std::any::Any;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::actor::ActorPath;

#[derive(Debug)]
pub struct DeadLetter {
    pub recipient: ActorPath,
    pub sender: Option<ActorPath>,
    pub message: Box<dyn Any + Send>,
}

#[derive(Default, Clone)]
pub struct DeadLettersSink {
    buf: Arc<Mutex<Vec<DeadLetter>>>,
}

impl DeadLettersSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, dl: DeadLetter) {
        tracing::warn!(recipient = %dl.recipient, "dead letter");
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

    #[test]
    fn stores_dead_letters() {
        let s = DeadLettersSink::new();
        s.push(DeadLetter {
            recipient: ActorPath::root(Address::local("S")).child("x"),
            sender: None,
            message: Box::new(1u32),
        });
        assert_eq!(s.len(), 1);
        let d = s.drain();
        assert_eq!(d.len(), 1);
    }
}
