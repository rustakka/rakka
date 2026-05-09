//! Process Manager implementation.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::topology::Topology;
use crate::PatternError;

/// What a process manager does in response to an event.
pub enum Transition<S, C> {
    /// Don't change state; dispatch no commands.
    Stay,
    /// Move to `next` state; dispatch `commands` in order.
    Goto { next: S, commands: Vec<C> },
    /// Terminal state — clear correlation, dispatch any final
    /// commands.
    Complete { commands: Vec<C> },
}

/// Typed state-machine process manager.
pub trait ProcessManager: Send + 'static {
    type Event: Send + Clone + 'static;
    type Command: Send + 'static;
    type State: Clone + Send + Default + 'static;
    type Error: std::error::Error + Send + 'static;

    fn correlation_id(event: &Self::Event) -> Option<String>;

    fn transition(
        state: &Self::State,
        event: Self::Event,
    ) -> Result<Transition<Self::State, Self::Command>, Self::Error>;
}

pub struct ProcessManagerPattern<P>(PhantomData<P>);

impl<P: ProcessManager> ProcessManagerPattern<P> {
    pub fn builder() -> ProcessManagerBuilder<P> {
        ProcessManagerBuilder { name: None, events: None, dispatcher: None }
    }
}

type DispatcherFn<C> =
    Arc<dyn Fn(C) -> futures::future::BoxFuture<'static, bool> + Send + Sync>;

pub struct ProcessManagerBuilder<P: ProcessManager> {
    name: Option<String>,
    events: Option<UnboundedReceiver<P::Event>>,
    dispatcher: Option<DispatcherFn<P::Command>>,
}

impl<P: ProcessManager> ProcessManagerBuilder<P> {
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }
    pub fn events(mut self, rx: UnboundedReceiver<P::Event>) -> Self {
        self.events = Some(rx);
        self
    }
    pub fn dispatcher<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(P::Command) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send + 'static,
    {
        let f = Arc::new(f);
        self.dispatcher = Some(Arc::new(move |c| {
            let f = f.clone();
            Box::pin(async move { f(c).await })
        }));
        self
    }
    pub fn build(self) -> Result<ProcessManagerTopology<P>, PatternError<P::Error>> {
        Ok(ProcessManagerTopology {
            name: self.name.unwrap_or_else(|| "process-manager".into()),
            events: self.events.ok_or(PatternError::NotConfigured("events"))?,
            dispatcher: self.dispatcher.ok_or(PatternError::NotConfigured("dispatcher"))?,
        })
    }
}

pub struct ProcessManagerTopology<P: ProcessManager> {
    name: String,
    events: UnboundedReceiver<P::Event>,
    dispatcher: DispatcherFn<P::Command>,
}

pub struct ProcessManagerHandles {
    pub name: String,
}

#[async_trait]
impl<P: ProcessManager> Topology for ProcessManagerTopology<P> {
    type Handles = ProcessManagerHandles;

    async fn materialize(self, _system: &ActorSystem) -> Result<Self::Handles, PatternError<()>> {
        let ProcessManagerTopology { name, mut events, dispatcher } = self;
        let task_name = name.clone();
        tokio::spawn(async move {
            let mut states: HashMap<String, P::State> = HashMap::new();
            while let Some(event) = events.recv().await {
                let Some(corr) = P::correlation_id(&event) else {
                    continue;
                };
                let state = states.entry(corr.clone()).or_default();
                match P::transition(state, event) {
                    Ok(Transition::Stay) => {}
                    Ok(Transition::Goto { next, commands }) => {
                        *state = next;
                        for c in commands {
                            let _ = (dispatcher)(c).await;
                        }
                    }
                    Ok(Transition::Complete { commands }) => {
                        for c in commands {
                            let _ = (dispatcher)(c).await;
                        }
                        states.remove(&corr);
                    }
                    Err(e) => {
                        tracing::warn!(pm = %task_name, error = %e, "transition failed");
                    }
                }
            }
        });
        Ok(ProcessManagerHandles { name })
    }
}
