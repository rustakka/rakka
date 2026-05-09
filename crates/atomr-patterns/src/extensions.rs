//! Extension hooks shared across patterns.
//!
//! Two flavors:
//!
//! 1. **Named slots** — typed closures that the framework runs at
//!    well-known points (`on_command`, `on_event`). These can fail and
//!    surface a [`crate::PatternError`].
//! 2. **Generic taps** — fire-and-forget [`tokio::sync::mpsc`] senders
//!    that receive a clone of every command/event. Use these to bridge
//!    into [`atomr_streams`] without coupling the pattern surface to a
//!    specific Source/Sink shape.

use std::sync::Arc;

use crate::PatternError;

/// A pre-handler interceptor. Receives the command by reference; may
/// reject it (turns into [`PatternError::Intercepted`]).
pub type CommandInterceptor<C, E> =
    Arc<dyn Fn(&C) -> Result<(), PatternError<E>> + Send + Sync + 'static>;

/// A post-persist event listener. Synchronous; fast hooks only — for
/// async work, push events into a tap channel and react out-of-band.
pub type EventListener<EV> = Arc<dyn Fn(&EV) + Send + Sync + 'static>;

/// Bundle of extension hooks. Reused by every pattern that wants to let
/// users plug in their own actors / sinks at well-known points.
pub struct ExtensionSlots<C, EV, DE> {
    pub command_interceptors: Vec<CommandInterceptor<C, DE>>,
    pub event_listeners: Vec<EventListener<EV>>,
    pub command_taps: Vec<tokio::sync::mpsc::UnboundedSender<C>>,
    pub event_taps: Vec<tokio::sync::mpsc::UnboundedSender<EV>>,
}

impl<C, EV, DE> Default for ExtensionSlots<C, EV, DE> {
    fn default() -> Self {
        Self {
            command_interceptors: Vec::new(),
            event_listeners: Vec::new(),
            command_taps: Vec::new(),
            event_taps: Vec::new(),
        }
    }
}

impl<C, EV, DE> Clone for ExtensionSlots<C, EV, DE> {
    fn clone(&self) -> Self {
        Self {
            command_interceptors: self.command_interceptors.clone(),
            event_listeners: self.event_listeners.clone(),
            command_taps: self.command_taps.clone(),
            event_taps: self.event_taps.clone(),
        }
    }
}

impl<C, EV, DE> ExtensionSlots<C, EV, DE> {
    /// Run every interceptor; bail on the first rejection.
    pub fn run_interceptors(&self, cmd: &C) -> Result<(), PatternError<DE>> {
        for hook in &self.command_interceptors {
            hook(cmd)?;
        }
        Ok(())
    }

    /// Notify every event listener.
    pub fn notify_listeners(&self, ev: &EV) {
        for hook in &self.event_listeners {
            hook(ev);
        }
    }
}

impl<C: Clone, EV, DE> ExtensionSlots<C, EV, DE> {
    /// Push a command clone to every command tap. Closed receivers are
    /// silently pruned.
    pub fn push_command_taps(&mut self, cmd: &C) {
        self.command_taps.retain(|tx| tx.send(cmd.clone()).is_ok());
    }
}

impl<C, EV: Clone, DE> ExtensionSlots<C, EV, DE> {
    /// Push an event clone to every event tap. Closed receivers are
    /// silently pruned.
    pub fn push_event_taps(&mut self, ev: &EV) {
        self.event_taps.retain(|tx| tx.send(ev.clone()).is_ok());
    }
}
