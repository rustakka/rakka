//! Saga implementation.
//!
//! v1 design: the saga listens to an event channel (typically wired
//! from a [`crate::cqrs::CqrsPattern`]'s `tap_events`) and dispatches
//! commands via a user-supplied `dispatcher` closure. State is kept in
//! a `HashMap<CorrelationId, Saga::State>` inside a tokio task.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use atomr_core::actor::ActorSystem;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::saga::state_store::{InMemorySagaStateStore, SagaStateStore};
use crate::topology::Topology;
use crate::PatternError;

/// What a saga decides to do in response to an event.
pub enum SagaAction<C> {
    /// Dispatch this command immediately.
    Send(C),
    /// Dispatch this command after a delay.
    Schedule(C, Duration),
    /// Dispatch a chain of compensating commands (rollback).
    Compensate(Vec<C>),
    /// The saga is done — clear its state.
    Complete,
}

/// User-defined saga / process manager.
#[async_trait]
pub trait Saga: Send + 'static {
    type Event: Send + Clone + 'static;
    type Command: Send + 'static;
    type State: Default + Send + 'static;
    type Error: std::error::Error + Send + 'static;

    /// Stable correlation key for `event`. `None` means the event is
    /// not for this saga.
    fn correlation_id(event: &Self::Event) -> Option<String>;

    /// React to an event. Receives mutable access to the per-saga state
    /// keyed by `correlation_id`.
    async fn handle(
        &mut self,
        state: &mut Self::State,
        event: Self::Event,
    ) -> Result<Vec<SagaAction<Self::Command>>, Self::Error>;

    /// Optional codec for state persistence. `None` keeps state
    /// in-memory only (default — preserves v1 behavior). Implement to
    /// participate in [`crate::saga::SagaStateStore`] persistence.
    fn encode_state(_state: &Self::State) -> Option<Result<Vec<u8>, String>> {
        None
    }

    /// Decode a persisted payload back into `State`. Required iff
    /// [`Self::encode_state`] is implemented.
    fn decode_state(_bytes: &[u8]) -> Result<Self::State, String> {
        Err("decode_state not implemented".into())
    }
}

/// Public, zero-sized handle for the saga pattern.
pub struct SagaPattern<S>(PhantomData<S>);

impl<S: Saga> SagaPattern<S> {
    /// Build a saga around the given event source and command dispatcher.
    /// `dispatcher` returns `true` on success — used to decide whether
    /// to invoke compensation.
    pub fn builder() -> SagaBuilder<S> {
        SagaBuilder::default()
    }
}

type SagaDispatcher<C> = Arc<dyn Fn(C) -> futures::future::BoxFuture<'static, bool> + Send + Sync>;

/// Fluent builder.
pub struct SagaBuilder<S: Saga> {
    name: Option<String>,
    saga: Option<S>,
    events: Option<UnboundedReceiver<S::Event>>,
    dispatcher: Option<SagaDispatcher<S::Command>>,
    state_store: Option<Arc<dyn SagaStateStore>>,
}

impl<S: Saga> Default for SagaBuilder<S> {
    fn default() -> Self {
        Self { name: None, saga: None, events: None, dispatcher: None, state_store: None }
    }
}

impl<S: Saga> SagaBuilder<S> {
    /// Override the actor name used for tracing / topology display.
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    /// Provide the saga implementation.
    pub fn saga(mut self, s: S) -> Self {
        self.saga = Some(s);
        self
    }

    /// Provide the event source. Typically wired from
    /// `CqrsBuilder::tap_events`.
    pub fn events(mut self, rx: UnboundedReceiver<S::Event>) -> Self {
        self.events = Some(rx);
        self
    }

    /// Provide the command dispatcher. The closure receives the
    /// command and returns whether the dispatch succeeded — failures
    /// cause [`SagaAction::Compensate`] chains to fire (when present).
    pub fn dispatcher<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(S::Command) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = bool> + Send + 'static,
    {
        let f = Arc::new(f);
        self.dispatcher = Some(Arc::new(move |cmd| {
            let f = f.clone();
            Box::pin(async move { f(cmd).await })
        }));
        self
    }

    /// Provide a [`SagaStateStore`]. When set together with
    /// [`Saga::encode_state`] / [`Saga::decode_state`], the runner
    /// reloads in-flight saga states on startup and persists state
    /// after each event handle. Default: in-memory.
    pub fn state_store<T: SagaStateStore>(mut self, store: Arc<T>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Finalize the builder.
    pub fn build(self) -> Result<SagaTopology<S>, PatternError<S::Error>> {
        let state_store: Arc<dyn SagaStateStore> =
            self.state_store.unwrap_or_else(|| Arc::new(InMemorySagaStateStore::new()));
        Ok(SagaTopology {
            name: self.name.unwrap_or_else(|| "saga".into()),
            saga: self.saga.ok_or(PatternError::NotConfigured("saga"))?,
            events: self.events.ok_or(PatternError::NotConfigured("events"))?,
            dispatcher: self.dispatcher.ok_or(PatternError::NotConfigured("dispatcher"))?,
            state_store,
        })
    }
}

/// Materializable description of a saga.
pub struct SagaTopology<S: Saga> {
    name: String,
    saga: S,
    events: UnboundedReceiver<S::Event>,
    dispatcher: SagaDispatcher<S::Command>,
    state_store: Arc<dyn SagaStateStore>,
}

/// Handles handed back after [`Topology::materialize`].
pub struct SagaHandles {
    pub name: String,
}

#[async_trait]
impl<S: Saga> Topology for SagaTopology<S> {
    type Handles = SagaHandles;

    async fn materialize(self, _system: &ActorSystem) -> Result<SagaHandles, PatternError<()>> {
        let SagaTopology { name, mut saga, mut events, dispatcher, state_store } = self;
        let task_name = name.clone();
        tokio::spawn(async move {
            let mut states: HashMap<String, S::State> = HashMap::new();
            // Rehydrate any persisted in-flight saga states.
            if S::encode_state(&S::State::default()).is_some() {
                for corr in state_store.keys().await {
                    if let Some(payload) = state_store.load(&corr).await {
                        match S::decode_state(&payload) {
                            Ok(state) => {
                                states.insert(corr, state);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    saga = %task_name,
                                    error = %e,
                                    "decode saga state failed; dropping"
                                );
                            }
                        }
                    }
                }
            }
            while let Some(event) = events.recv().await {
                let Some(corr) = S::correlation_id(&event) else {
                    continue;
                };
                let state = states.entry(corr.clone()).or_default();
                match saga.handle(state, event).await {
                    Ok(actions) => {
                        // Persist updated state before any dispatch so a
                        // crash mid-dispatch doesn't lose the decision.
                        if let Some(Ok(payload)) = S::encode_state(state) {
                            state_store.save(&corr, payload).await;
                        }
                        let mut completed = false;
                        for action in actions {
                            match action {
                                SagaAction::Send(c) => {
                                    let _ = (dispatcher)(c).await;
                                }
                                SagaAction::Schedule(c, delay) => {
                                    let dispatcher = dispatcher.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(delay).await;
                                        let _ = (dispatcher)(c).await;
                                    });
                                }
                                SagaAction::Compensate(cs) => {
                                    for c in cs {
                                        let _ = (dispatcher)(c).await;
                                    }
                                }
                                SagaAction::Complete => {
                                    completed = true;
                                    break;
                                }
                            }
                        }
                        if completed {
                            states.remove(&corr);
                            state_store.delete(&corr).await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(saga = %task_name, error = %e, "saga handle failed");
                    }
                }
            }
        });
        Ok(SagaHandles { name })
    }
}
