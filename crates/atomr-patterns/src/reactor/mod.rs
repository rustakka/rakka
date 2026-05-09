//! Reactor pattern — fire-and-forget side effects in response to events.
//!
//! Lighter than [`crate::saga::SagaPattern`]: no per-correlation
//! state, no command dispatch, no compensation. Just "for each event,
//! run this side-effect closure." Commonly used for notifications,
//! metrics emission, log aggregation.
//!
//! ```ignore
//! ReactorPattern::<OrderEvent>::builder()
//!     .name("notifier")
//!     .events(bus.subscribe())
//!     .reaction(|e| async move { send_notification(e).await; })
//!     .build()?
//!     .materialize(&system)
//!     .await?;
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::topology::Topology;
use crate::PatternError;

type Reaction<E> = Arc<dyn Fn(E) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

pub struct ReactorPattern<E>(PhantomData<E>);

impl<E: Send + 'static> ReactorPattern<E> {
    pub fn builder() -> ReactorBuilder<E> {
        ReactorBuilder { name: None, events: None, reaction: None }
    }
}

pub struct ReactorBuilder<E: Send + 'static> {
    name: Option<String>,
    events: Option<UnboundedReceiver<E>>,
    reaction: Option<Reaction<E>>,
}

impl<E: Send + 'static> ReactorBuilder<E> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }
    pub fn events(mut self, rx: UnboundedReceiver<E>) -> Self {
        self.events = Some(rx);
        self
    }
    pub fn reaction<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let f = Arc::new(f);
        self.reaction = Some(Arc::new(move |e| {
            let f = f.clone();
            Box::pin(async move { f(e).await })
        }));
        self
    }
    pub fn build(self) -> Result<ReactorTopology<E>, PatternError<()>> {
        Ok(ReactorTopology {
            name: self.name.unwrap_or_else(|| "reactor".into()),
            events: self.events.ok_or(PatternError::NotConfigured("events"))?,
            reaction: self.reaction.ok_or(PatternError::NotConfigured("reaction"))?,
        })
    }
}

pub struct ReactorTopology<E: Send + 'static> {
    name: String,
    events: UnboundedReceiver<E>,
    reaction: Reaction<E>,
}

pub struct ReactorHandles {
    pub name: String,
}

#[async_trait]
impl<E: Send + 'static> Topology for ReactorTopology<E> {
    type Handles = ReactorHandles;
    async fn materialize(self, _system: &ActorSystem) -> Result<ReactorHandles, PatternError<()>> {
        let ReactorTopology { name, mut events, reaction } = self;
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                (reaction)(event).await;
            }
        });
        Ok(ReactorHandles { name })
    }
}
