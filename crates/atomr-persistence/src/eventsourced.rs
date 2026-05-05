//! `Eventsourced` — the modern command/event/state trait.
//!
//! Improves on the legacy [`crate::PersistentActor`] in three ways:
//!
//! 1. **Typed errors via `thiserror`** — handlers return
//!    `Result<Vec<Event>, Self::Error>` so domain rejections
//!    short-circuit without panicking.
//! 2. **`recovery_completed` lifecycle hook** so actors can warm
//!    caches / register subscriptions once replay is done.
//! 3. **Pluggable codec via the trait** — `encode_event` /
//!    `decode_event` return `Result` and use a configurable codec
//!    name baked into each `PersistentRepr.manifest`.
//!
//! `PersistentActor` remains in place for back-compat; new actors
//! should target this trait.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::journal::{Journal, JournalError, PersistentRepr};
use crate::recovery_permitter::RecoveryPermitter;
use crate::snapshot::{SnapshotMetadata, SnapshotStore};

/// Recovery / handler errors that propagate out of [`Eventsourced`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EventsourcedError<DomainErr> {
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("recovery permit acquire failed")]
    PermitDenied,
    #[error(transparent)]
    Domain(DomainErr),
}

/// Modern event-sourced actor.
#[async_trait]
pub trait Eventsourced: Send + 'static {
    /// User commands received via `handle_command`.
    type Command: Send + 'static;
    /// Persisted events derived from commands by `command_to_events`.
    type Event: Send + Clone + 'static;
    /// In-memory state mutated by `apply_event`.
    type State: Default + Send + 'static;
    /// Domain-level errors a command handler can return.
    type Error: std::error::Error + Send + 'static;

    /// Stable journal key for this actor instance.
    fn persistence_id(&self) -> String;

    /// Manifest tag baked into each `PersistentRepr` so cross-version
    /// replay can dispatch to the right decoder. Defaults to `"evt"`.
    fn event_manifest(&self) -> &'static str {
        "evt"
    }

    /// Pure projection of a command into 0..N events. Validation /
    /// rejection lives here (`Err(_)` aborts the persist).
    fn command_to_events(
        &self,
        state: &Self::State,
        cmd: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error>;

    /// Apply a persisted event to in-memory state. Called both during
    /// recovery (per replayed event) and during normal operation
    /// (after each successful persist).
    fn apply_event(state: &mut Self::State, event: &Self::Event);

    /// Encode an event for the journal. Errors short-circuit the
    /// persist with [`EventsourcedError::Codec`].
    fn encode_event(event: &Self::Event) -> Result<Vec<u8>, String>;

    /// Decode an event from journal bytes. Symmetric with
    /// [`Self::encode_event`].
    fn decode_event(bytes: &[u8]) -> Result<Self::Event, String>;

    /// Lifecycle hook fired after recovery completes. Default no-op.
    async fn recovery_completed(&mut self, _state: &Self::State, _highest_seq: u64) {}

    // ---- Driver methods (default implementations) -----------------

    /// Replay the journal under a [`RecoveryPermitter`], applying
    /// each event to `state`. Returns the highest replayed
    /// sequence number.
    async fn recover<J: Journal>(
        &mut self,
        journal: Arc<J>,
        state: &mut Self::State,
        permitter: &RecoveryPermitter,
    ) -> Result<u64, EventsourcedError<Self::Error>> {
        let _permit = permitter.acquire().await.ok_or(EventsourcedError::PermitDenied)?;
        let pid = self.persistence_id();
        let highest = journal.highest_sequence_nr(&pid, 0).await?;
        let events = journal.replay_messages(&pid, 1, highest, u64::MAX).await?;
        for e in &events {
            let evt = Self::decode_event(&e.payload).map_err(EventsourcedError::Codec)?;
            Self::apply_event(state, &evt);
        }
        // Permit dropped here, freeing a slot for the next recovering
        // actor before we run the user-facing hook.
        drop(_permit);
        self.recovery_completed(state, highest).await;
        Ok(highest)
    }

    /// Run a single command — derive events, persist, apply.
    async fn handle_command<J: Journal>(
        &self,
        journal: Arc<J>,
        state: &mut Self::State,
        next_seq: &mut u64,
        writer_uuid: &str,
        cmd: Self::Command,
    ) -> Result<(), EventsourcedError<Self::Error>> {
        let events = self.command_to_events(state, cmd).map_err(EventsourcedError::Domain)?;
        if events.is_empty() {
            return Ok(());
        }
        let mut reprs = Vec::with_capacity(events.len());
        for e in &events {
            *next_seq += 1;
            let payload = Self::encode_event(e).map_err(EventsourcedError::Codec)?;
            reprs.push(PersistentRepr {
                persistence_id: self.persistence_id(),
                sequence_nr: *next_seq,
                payload,
                manifest: self.event_manifest().to_string(),
                writer_uuid: writer_uuid.into(),
                deleted: false,
                tags: Vec::new(),
            });
        }
        journal.write_messages(reprs).await?;
        for e in &events {
            Self::apply_event(state, e);
        }
        Ok(())
    }

    /// Save a snapshot of current state under `sequence_nr`.
    async fn save_snapshot<S: SnapshotStore>(&self, store: Arc<S>, sequence_nr: u64, payload: Vec<u8>) {
        store
            .save(
                SnapshotMetadata { persistence_id: self.persistence_id(), sequence_nr, timestamp: 0 },
                payload,
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryJournal, Journal};

    #[derive(Default, Debug, PartialEq)]
    struct CounterState {
        n: i64,
    }

    #[derive(Clone, Debug)]
    enum CounterEvent {
        Adjusted(i64),
    }

    enum CounterCmd {
        Add(i64),
        Sub(i64),
    }

    #[derive(Debug, Error)]
    enum CounterErr {
        #[error("would underflow below 0")]
        Underflow,
    }

    struct Counter {
        id: String,
    }

    #[async_trait]
    impl Eventsourced for Counter {
        type Command = CounterCmd;
        type Event = CounterEvent;
        type State = CounterState;
        type Error = CounterErr;

        fn persistence_id(&self) -> String {
            self.id.clone()
        }

        fn command_to_events(
            &self,
            state: &Self::State,
            cmd: Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            let delta = match cmd {
                CounterCmd::Add(n) => n,
                CounterCmd::Sub(n) => -n,
            };
            if state.n + delta < 0 {
                return Err(CounterErr::Underflow);
            }
            Ok(vec![CounterEvent::Adjusted(delta)])
        }

        fn apply_event(state: &mut Self::State, event: &Self::Event) {
            match event {
                CounterEvent::Adjusted(d) => state.n += d,
            }
        }

        fn encode_event(event: &Self::Event) -> Result<Vec<u8>, String> {
            match event {
                CounterEvent::Adjusted(d) => Ok(d.to_le_bytes().to_vec()),
            }
        }

        fn decode_event(bytes: &[u8]) -> Result<Self::Event, String> {
            if bytes.len() != 8 {
                return Err(format!("bad len: {}", bytes.len()));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(bytes);
            Ok(CounterEvent::Adjusted(i64::from_le_bytes(buf)))
        }
    }

    #[tokio::test]
    async fn happy_path_persist_and_recover() {
        let journal = Arc::new(InMemoryJournal::default());
        let permitter = RecoveryPermitter::new(2);

        // First incarnation: persist three commands.
        let c = Counter { id: "c-1".into() };
        let mut state = CounterState::default();
        let mut seq = 0u64;
        c.handle_command(journal.clone(), &mut state, &mut seq, "w", CounterCmd::Add(5)).await.unwrap();
        c.handle_command(journal.clone(), &mut state, &mut seq, "w", CounterCmd::Add(3)).await.unwrap();
        c.handle_command(journal.clone(), &mut state, &mut seq, "w", CounterCmd::Sub(2)).await.unwrap();
        assert_eq!(state.n, 6);
        assert_eq!(seq, 3);
        let highest = journal.highest_sequence_nr("c-1", 0).await.unwrap();
        assert_eq!(highest, 3);

        // Second incarnation: replay → state should match.
        let mut c2 = Counter { id: "c-1".into() };
        let mut state2 = CounterState::default();
        let h = c2.recover(journal.clone(), &mut state2, &permitter).await.unwrap();
        assert_eq!(h, 3);
        assert_eq!(state2.n, 6);
    }

    #[tokio::test]
    async fn domain_error_aborts_persist() {
        let journal = Arc::new(InMemoryJournal::default());
        let c = Counter { id: "c-2".into() };
        let mut state = CounterState::default();
        let mut seq = 0u64;
        let r = c.handle_command(journal.clone(), &mut state, &mut seq, "w", CounterCmd::Sub(5)).await;
        assert!(matches!(r, Err(EventsourcedError::Domain(CounterErr::Underflow))));
        assert_eq!(seq, 0);
        assert_eq!(journal.highest_sequence_nr("c-2", 0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn recovery_completed_called_once() {
        struct HookCounter {
            id: String,
            hook_calls: Arc<std::sync::atomic::AtomicU32>,
        }
        #[async_trait]
        impl Eventsourced for HookCounter {
            type Command = ();
            type Event = ();
            type State = ();
            type Error = std::io::Error;
            fn persistence_id(&self) -> String {
                self.id.clone()
            }
            fn command_to_events(&self, _: &(), _: ()) -> Result<Vec<()>, Self::Error> {
                Ok(vec![])
            }
            fn apply_event(_: &mut (), _: &()) {}
            fn encode_event(_: &()) -> Result<Vec<u8>, String> {
                Ok(vec![])
            }
            fn decode_event(_: &[u8]) -> Result<(), String> {
                Ok(())
            }
            async fn recovery_completed(&mut self, _: &(), _: u64) {
                self.hook_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let journal = Arc::new(InMemoryJournal::default());
        let permitter = RecoveryPermitter::new(1);
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut a = HookCounter { id: "h".into(), hook_calls: calls.clone() };
        let _ = a.recover(journal, &mut (), &permitter).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
