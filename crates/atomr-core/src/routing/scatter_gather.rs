//! Scatter-gather-first-completed router.

use std::time::Duration;

use tokio::sync::oneshot;

use crate::actor::{ActorRef, AskError};

pub struct ScatterGatherFirstCompletedRouter<M: Send + 'static> {
    pub routees: Vec<ActorRef<M>>,
    pub within: Duration,
}

impl<M: Send + 'static> ScatterGatherFirstCompletedRouter<M> {
    pub fn new(routees: Vec<ActorRef<M>>, within: Duration) -> Self {
        Self { routees, within }
    }

    /// Fan out a query and return the first non-error reply.
    pub async fn ask_first<R, F>(&self, mut build: F) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnMut(oneshot::Sender<R>) -> M,
    {
        let mut joins = futures_util::stream::FuturesUnordered::new();
        for r in &self.routees {
            let (tx, rx) = oneshot::channel::<R>();
            r.tell(build(tx));
            joins.push(async move { tokio::time::timeout(self.within, rx).await });
        }
        use futures_util::StreamExt;
        while let Some(res) = joins.next().await {
            match res {
                Ok(Ok(v)) => return Ok(v),
                _ => continue,
            }
        }
        Err(AskError::Timeout)
    }
}
