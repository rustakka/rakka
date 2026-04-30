//! Core `Actor` trait and message envelope.
//! akka.net: `Actor/ActorBase.cs`, `ReceiveActor.cs`.

use std::any::Any;

use async_trait::async_trait;

use super::context::Context;
use crate::supervision::SupervisorStrategy;

/// Envelope that carries a user message plus an optional sender.
///
/// `M` is the actor's user message type. Akka.NET has `Sender`; here we
/// carry an opaque `Box<dyn Any + Send>` for sender type erasure, because
/// the sender ref's type is generally unknown to the recipient.
pub struct MessageEnvelope<M> {
    pub message: M,
    pub sender: Option<Box<dyn Any + Send>>,
}

impl<M> MessageEnvelope<M> {
    pub fn new(message: M) -> Self {
        Self { message, sender: None }
    }

    pub fn with_sender<S: Any + Send>(message: M, sender: S) -> Self {
        Self { message, sender: Some(Box::new(sender)) }
    }
}

/// The user-facing `Actor` trait.
///
/// Akka.NET's `ReceiveActor` is expressed here as: each actor has an
/// associated `Msg` type (typically an enum) and implements an async
/// `handle` that matches on it.
#[async_trait]
pub trait Actor: Sized + Send + 'static {
    type Msg: Send + 'static;

    /// Process a single message.
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg);

    /// Called once before the first message (akka.net: `PreStart`).
    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {}

    /// Called after the actor has been stopped (akka.net: `PostStop`).
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
