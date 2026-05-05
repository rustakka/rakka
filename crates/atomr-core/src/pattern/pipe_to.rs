//! `PipeTo` — send the eventual output of a future to an actor.

use std::future::Future;

use crate::actor::ActorRef;

pub fn pipe_to<M, F>(fut: F, target: ActorRef<M>)
where
    M: Send + 'static,
    F: Future<Output = M> + Send + 'static,
{
    tokio::spawn(async move {
        let m = fut.await;
        target.tell(m);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Inbox;
    use std::time::Duration;

    #[tokio::test]
    async fn pipes_future_to_actor() {
        let mut inbox = Inbox::<u32>::new("pipe");
        pipe_to(async { 42u32 }, inbox.actor_ref().clone());
        let m = inbox.receive(Duration::from_millis(100)).await.unwrap();
        assert_eq!(m, 42);
    }
}
