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
        if pred(&m) {
            Ok(m)
        } else {
            Err(TestProbeError::Unexpected)
        }
    }

    /// Assert that no message arrives within the given timeout.
    pub async fn expect_no_msg(&mut self, timeout: Duration) -> Result<(), TestProbeError> {
        match tokio::time::timeout(timeout, self.inbox.receive(Duration::from_secs(3600))).await {
            Ok(_) => Err(TestProbeError::Unexpected),
            Err(_) => Ok(()),
        }
    }

    // -- Phase 4 matchers ------------------------------------------

    /// Wait for a message and assert it matches the variant returned
    /// by `extract`. Akka.NET: `ExpectMsg<T>(...)` where `T` selects
    /// a sub-variant of the message enum. The `extract` closure
    /// returns `Some(payload)` for the desired variant.
    pub async fn expect_msg_class<T, F>(&mut self, timeout: Duration, extract: F) -> Result<T, TestProbeError>
    where
        F: FnOnce(M) -> Option<T>,
    {
        let m = self.expect_msg(timeout).await?;
        extract(m).ok_or(TestProbeError::Unexpected)
    }

    /// Receive exactly `n` messages or return [`TestProbeError::Timeout`]
    /// if `timeout` elapses before they all arrive.
    /// Akka.NET: `ReceiveN(int n, TimeSpan)`.
    pub async fn receive_n(&mut self, n: usize, timeout: Duration) -> Result<Vec<M>, TestProbeError> {
        let deadline = std::time::Instant::now() + timeout;
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let remaining =
                deadline.checked_duration_since(std::time::Instant::now()).ok_or(TestProbeError::Timeout)?;
            out.push(self.expect_msg(remaining).await?);
        }
        Ok(out)
    }

    /// Receive messages while `pred` returns true, stopping at the
    /// first message for which `pred` returns false (that message is
    /// discarded). Akka.NET: `ReceiveWhile`.
    pub async fn receive_while<F>(&mut self, timeout: Duration, mut pred: F) -> Result<Vec<M>, TestProbeError>
    where
        F: FnMut(&M) -> bool,
    {
        let deadline = std::time::Instant::now() + timeout;
        let mut out = Vec::new();
        loop {
            let remaining = match deadline.checked_duration_since(std::time::Instant::now()) {
                Some(d) => d,
                None => return Ok(out),
            };
            match self.expect_msg(remaining).await {
                Ok(m) => {
                    if pred(&m) {
                        out.push(m);
                    } else {
                        return Ok(out);
                    }
                }
                Err(TestProbeError::Timeout) => return Ok(out),
                Err(e) => return Err(e),
            }
        }
    }

    /// Drain messages until one matches `pred`. Discards mismatches.
    /// Akka.NET: `FishForMessage`.
    pub async fn fish_for_message<F>(&mut self, timeout: Duration, mut pred: F) -> Result<M, TestProbeError>
    where
        F: FnMut(&M) -> bool,
    {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let remaining =
                deadline.checked_duration_since(std::time::Instant::now()).ok_or(TestProbeError::Timeout)?;
            let m = self.expect_msg(remaining).await?;
            if pred(&m) {
                return Ok(m);
            }
        }
    }

    /// Receive `expected.len()` messages and assert that the multi-set
    /// of received messages equals `expected` (order-insensitive).
    /// Akka.NET: `ExpectMsgAllOf`.
    pub async fn expect_all_of(&mut self, timeout: Duration, expected: Vec<M>) -> Result<(), TestProbeError>
    where
        M: PartialEq + std::fmt::Debug,
    {
        let n = expected.len();
        let received = self.receive_n(n, timeout).await?;
        // O(n²) intentional — n is small in practice.
        let mut remaining: Vec<M> = received;
        for want in expected {
            if let Some(idx) = remaining.iter().position(|m| m == &want) {
                remaining.remove(idx);
            } else {
                return Err(TestProbeError::Unexpected);
            }
        }
        Ok(())
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

    #[tokio::test]
    async fn receive_n_collects_messages() {
        let mut p = TestProbe::<u32>::new("rn");
        for i in 0..3u32 {
            p.actor_ref().tell(i);
        }
        let msgs = p.receive_n(3, Duration::from_millis(100)).await.unwrap();
        assert_eq!(msgs, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn receive_n_times_out_partial() {
        let mut p = TestProbe::<u32>::new("rnt");
        p.actor_ref().tell(7);
        let r = p.receive_n(3, Duration::from_millis(20)).await;
        assert!(matches!(r, Err(TestProbeError::Timeout)));
    }

    #[tokio::test]
    async fn fish_for_message_skips_mismatches() {
        let mut p = TestProbe::<u32>::new("fish");
        p.actor_ref().tell(1);
        p.actor_ref().tell(2);
        p.actor_ref().tell(99);
        let m = p.fish_for_message(Duration::from_millis(100), |m| *m >= 50).await.unwrap();
        assert_eq!(m, 99);
    }

    #[tokio::test]
    async fn receive_while_stops_on_predicate() {
        let mut p = TestProbe::<u32>::new("rw");
        for i in 1..=4u32 {
            p.actor_ref().tell(i);
        }
        let collected = p.receive_while(Duration::from_millis(100), |m| *m < 3).await.unwrap();
        assert_eq!(collected, vec![1, 2]);
    }

    #[tokio::test]
    async fn expect_all_of_order_insensitive() {
        let mut p = TestProbe::<u32>::new("alf");
        for i in [3u32, 1, 2] {
            p.actor_ref().tell(i);
        }
        p.expect_all_of(Duration::from_millis(100), vec![1, 2, 3]).await.unwrap();
    }

    #[tokio::test]
    async fn expect_msg_class_extracts_variant() {
        #[derive(Debug, PartialEq)]
        #[allow(dead_code)]
        enum E {
            A(u32),
            B(String),
        }
        let mut p = TestProbe::<E>::new("cls");
        p.actor_ref().tell(E::B("hi".into()));
        let s = p
            .expect_msg_class(Duration::from_millis(100), |m| match m {
                E::B(s) => Some(s),
                _ => None,
            })
            .await
            .unwrap();
        assert_eq!(s, "hi");
    }
}
