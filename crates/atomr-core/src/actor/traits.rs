//! Core `Actor` trait and message envelope.

use async_trait::async_trait;

use super::context::Context;
use super::sender::Sender;
use crate::supervision::SupervisorStrategy;

/// Envelope that carries a user message plus a typed [`Sender`].
///
/// `M` is the actor's user message type. The [`Sender`] preserves the
/// origin's identity end-to-end (no `Any::downcast` on reply paths) —
/// see `docs/idiomatic-rust.md` (P-1) and Phase 1 of
/// `docs/full-port-plan.md`.
pub struct MessageEnvelope<M> {
    pub message: M,
    pub sender: Sender,
}

impl<M> MessageEnvelope<M> {
    pub fn new(message: M) -> Self {
        Self { message, sender: Sender::None }
    }

    /// Construct with a typed [`Sender`].
    pub fn with_typed_sender(message: M, sender: Sender) -> Self {
        Self { message, sender }
    }
}

/// The user-facing `Actor` trait.
///
/// is expressed here as: each actor has an
/// associated `Msg` type (typically an enum) and implements an async
/// `handle` that matches on it.
#[async_trait]
pub trait Actor: Sized + Send + 'static {
    type Msg: Send + 'static;

    /// Process a single message.
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg);

    /// Called once before the first message.
    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {}

    /// Called after the actor has been stopped.
    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {}

    /// Called when the actor is about to be restarted by the supervisor.
    async fn pre_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}

    /// Called after a restart.
    async fn post_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}

    /// The supervisor strategy this actor applies to its own children.
    fn supervisor_strategy(&self) -> SupervisorStrategy {
        SupervisorStrategy::default()
    }
}
