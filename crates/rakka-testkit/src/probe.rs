//! `TestProbe` — typed message receiver used in assertions.
//! akka.net: `Akka.TestKit/TestProbe.cs`.

use std::time::Duration;

use rakka_core::actor::Inbox;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TestProbeError {
    #[error("probe timed out waiting for message")]
    Timeout,
    #[error("probe sender dropped")]
    Dropped,
    #[error("unexpected message")]
    Unexpected,
}

pub struct TestProbe<M: Send + 'static> {
    inbox: Inbox<M>,
}

impl<M: Send + 'static> TestProbe<M> {
    pub fn new(name: &str) -> Self {
        Self { inbox: Inbox::new(name) }
    }

    pub fn actor_ref(&self) -> &rakka_core::actor::ActorRef<M> {
        self.inbox.actor_ref()
    }

    /// Wait for a single message (akka.net: `ExpectMsg`).
    pub async fn expect_msg(&mut self, timeout: Duration) -> Result<M, TestProbeError> {
        match self.inbox.receive(timeout).await {
            Ok(m) => Ok(m),
            Err(rakka_core::actor::AskError::Timeout) => Err(TestProbeError::Timeout),
            Err(_) => Err(TestProbeError::Dropped),
        }
    }

    /// Wait for a message that matches the given predicate.
    /// akka.net: `ExpectMsg<T>(Func<T, bool>)`.
    pub async fn expect_msg_pf<F>(&mut self, timeout: Duration, mut pred: F) -> Result<M, TestProbeError>
    where
        F: FnMut(&M) -> bool,
    {
        let m = self.expect_msg(timeout).await?;
        if pred(&m) { Ok(m) } else { Err(TestProbeError::Unexpected) }
    }

    /// Assert that no message arrives within the given timeout.
    pub async fn expect_no_msg(&mut self, timeout: Duration) -> Result<(), TestProbeError> {
        match tokio::time::timeout(timeout, self.inbox.receive(Duration::from_secs(3600))).await {
            Ok(_) => Err(TestProbeError::Unexpected),
            Err(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_receives_message() {
        let mut p = TestProbe::<u32>::new("p");
        p.actor_ref().tell(42);
        let m = p.expect_msg(Duration::from_millis(100)).await.unwrap();
        assert_eq!(m, 42);
    }

    #[tokio::test]
    async fn probe_no_msg() {
        let mut p = TestProbe::<u32>::new("q");
        p.expect_no_msg(Duration::from_millis(20)).await.unwrap();
    }
}
