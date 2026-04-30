//! Inbox — a receive-from-the-outside handle into a synthetic actor.
//! akka.net: `Actor/Inbox.cs`.

use std::sync::Weak;
use std::time::Duration;

use tokio::sync::mpsc;

use super::actor_ref::{ActorRef, AskError};
use super::address::Address;
use super::path::ActorPath;
use super::traits::MessageEnvelope;

/// A one-shot-style receive handle useful in tests and top-level `main` code.
pub struct Inbox<M: Send + 'static> {
    rx: mpsc::UnboundedReceiver<MessageEnvelope<M>>,
    actor_ref: ActorRef<M>,
}

impl<M: Send + 'static> Inbox<M> {
    pub fn new(name: &str) -> Self {
        let (user_tx, rx) = mpsc::unbounded_channel::<MessageEnvelope<M>>();
        let (sys_tx, _sys_rx) = mpsc::unbounded_channel();
        let path = ActorPath::root(Address::local("Inbox")).child(name);
        let actor_ref = ActorRef::new(path, user_tx, sys_tx, Weak::new());
        Self { rx, actor_ref }
    }

    pub fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    pub async fn receive(&mut self, timeout: Duration) -> Result<M, AskError> {
        match tokio::time::timeout(timeout, self.rx.recv()).await {
            Ok(Some(env)) => Ok(env.message),
            Ok(None) => Err(AskError::TargetDropped),
            Err(_) => Err(AskError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inbox_receives_tell() {
        let mut inbox = Inbox::<u32>::new("t");
        inbox.actor_ref().tell(7);
        let m = inbox.receive(Duration::from_millis(100)).await.unwrap();
        assert_eq!(m, 7);
    }
}
