//! `ReceivePersistent` — closure-style helper for persistent actors.
//!
//! Phase 11.D of `docs/full-port-plan.md`. Akka.NET parity:
//! `Akka.Persistence.ReceivePersistentActor`. Where [`crate::Eventsourced`]
//! makes the user implement a trait, `ReceivePersistent` lets ad-hoc /
//! script-shaped actors register handler closures up front and run a
//! command loop without a custom struct.
//!
//! ```ignore
//! use rakka_persistence::{ReceivePersistent, RecoveryPermitter, InMemoryJournal};
//! # async fn ex() {
//! let journal = std::sync::Arc::new(InMemoryJournal::default());
//! let mut rp: ReceivePersistent<i64, i64, &'static str> = ReceivePersistent::new("counter")
//!     .on_command(|state, cmd| Ok(vec![cmd]))
//!     .on_event(|state, evt| { *state += evt; })
//!     .with_codec(
//!         |e| Ok(e.to_le_bytes().to_vec()),
//!         |b| Ok(i64::from_le_bytes(b.try_into().map_err(|_| "len".to_string())?)),
//!     );
//! let permits = RecoveryPermitter::new(1);
//! rp.recover(journal.clone(), &permits).await.unwrap();
//! rp.handle(journal.clone(), 5).await.unwrap();
//! assert_eq!(rp.state(), &5);
//! # }
//! ```

use std::sync::Arc;

use crate::eventsourced::EventsourcedError;
use crate::journal::{Journal, PersistentRepr};
use crate::recovery_permitter::RecoveryPermitter;

type CommandFn<S, C, E, Err> = Box<dyn FnMut(&S, C) -> Result<Vec<E>, Err> + Send>;
type EventFn<S, E> = Box<dyn FnMut(&mut S, &E) + Send>;
type EncodeFn<E> = Box<dyn Fn(&E) -> Result<Vec<u8>, String> + Send + Sync>;
type DecodeFn<E> = Box<dyn Fn(&[u8]) -> Result<E, String> + Send + Sync>;

/// Closure-style persistent actor.
pub struct ReceivePersistent<S, E, Err>
where
    S: Default + Send + 'static,
    E: Clone + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    persistence_id: String,
    state: S,
    next_seq: u64,
    writer_uuid: String,
    on_command: Option<CommandFn<S, E, E, Err>>,
    on_event: Option<EventFn<S, E>>,
    encode: Option<EncodeFn<E>>,
    decode: Option<DecodeFn<E>>,
}

impl<S, E, Err> ReceivePersistent<S, E, Err>
where
    S: Default + Send + 'static,
    E: Clone + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    pub fn new(persistence_id: impl Into<String>) -> Self {
        Self {
            persistence_id: persistence_id.into(),
            state: S::default(),
            next_seq: 0,
            writer_uuid: format!("{}-{}", std::process::id(), uuid_v4_simple()),
            on_command: None,
            on_event: None,
            encode: None,
            decode: None,
        }
    }

    /// Register the command handler. The closure receives the current
    /// state + the incoming command (the command type matches the
    /// event type for this minimal helper; richer command-vs-event
    /// shapes use `Eventsourced` directly).
    pub fn on_command<F>(mut self, f: F) -> Self
    where
        F: FnMut(&S, E) -> Result<Vec<E>, Err> + Send + 'static,
    {
        self.on_command = Some(Box::new(f));
        self
    }

    /// Register the event applier — mutates state in-place.
    pub fn on_event<F>(mut self, f: F) -> Self
    where
        F: FnMut(&mut S, &E) + Send + 'static,
    {
        self.on_event = Some(Box::new(f));
        self
    }

    /// Register the codec used to round-trip events through the journal.
    pub fn with_codec<EncF, DecF>(mut self, encode: EncF, decode: DecF) -> Self
    where
        EncF: Fn(&E) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        DecF: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        self.encode = Some(Box::new(encode));
        self.decode = Some(Box::new(decode));
        self
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn persistence_id(&self) -> &str {
        &self.persistence_id
    }

    /// Replay the journal under `permitter` and apply each event.
    pub async fn recover<J: Journal>(
        &mut self,
        journal: Arc<J>,
        permitter: &RecoveryPermitter,
    ) -> Result<u64, EventsourcedError<Err>> {
        let _permit = permitter
            .acquire()
            .await
            .ok_or(EventsourcedError::PermitDenied)?;
        let on_event = self
            .on_event
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_event handler not registered".into()))?;
        let decode = self
            .decode
            .as_ref()
            .ok_or_else(|| EventsourcedError::Codec("decoder not registered".into()))?;
        let highest = journal.highest_sequence_nr(&self.persistence_id, 0).await?;
        let events = journal
            .replay_messages(&self.persistence_id, 1, highest, u64::MAX)
            .await?;
        for e in &events {
            let evt = decode(&e.payload).map_err(EventsourcedError::Codec)?;
            on_event(&mut self.state, &evt);
        }
        self.next_seq = highest;
        Ok(highest)
    }

    /// Apply one command — derive events, persist, apply.
    pub async fn handle<J: Journal>(
        &mut self,
        journal: Arc<J>,
        cmd: E,
    ) -> Result<(), EventsourcedError<Err>> {
        let on_cmd = self
            .on_command
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_command handler not registered".into()))?;
        let events = on_cmd(&self.state, cmd).map_err(EventsourcedError::Domain)?;
        if events.is_empty() {
            return Ok(());
        }
        let on_event = self
            .on_event
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_event handler not registered".into()))?;
        let encode = self
            .encode
            .as_ref()
            .ok_or_else(|| EventsourcedError::Codec("encoder not registered".into()))?;
        let mut reprs = Vec::with_capacity(events.len());
        for e in &events {
            self.next_seq += 1;
            let payload = encode(e).map_err(EventsourcedError::Codec)?;
            reprs.push(PersistentRepr {
                persistence_id: self.persistence_id.clone(),
                sequence_nr: self.next_seq,
                payload,
                manifest: "evt".into(),
                writer_uuid: self.writer_uuid.clone(),
                deleted: false,
                tags: Vec::new(),
            });
        }
        journal.write_messages(reprs).await?;
        for e in &events {
            on_event(&mut self.state, e);
        }
        Ok(())
    }
}

fn uuid_v4_simple() -> String {
    // Tiny non-cryptographic id for writer_uuid. Good enough for
    // dedup purposes — the journal only uses this to disambiguate
    // concurrent writers.
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryJournal;

    #[derive(Debug, thiserror::Error)]
    #[error("dummy")]
    struct DummyErr;

    #[tokio::test]
    async fn closure_actor_persists_and_recovers() {
        let journal = Arc::new(InMemoryJournal::default());
        let permits = RecoveryPermitter::new(1);

        let mut rp: ReceivePersistent<i64, i64, DummyErr> = ReceivePersistent::new("pid-1")
            .on_command(|_state, cmd| Ok(vec![cmd]))
            .on_event(|state, evt| { *state += evt; })
            .with_codec(
                |e: &i64| Ok(e.to_le_bytes().to_vec()),
                |b: &[u8]| {
                    let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
                    Ok(i64::from_le_bytes(arr))
                },
            );

        rp.handle(journal.clone(), 5).await.unwrap();
        rp.handle(journal.clone(), 3).await.unwrap();
        rp.handle(journal.clone(), -2).await.unwrap();
        assert_eq!(rp.state(), &6);

        // Fresh replay reaches the same state.
        let mut rp2: ReceivePersistent<i64, i64, DummyErr> = ReceivePersistent::new("pid-1")
            .on_command(|_state, cmd| Ok(vec![cmd]))
            .on_event(|state, evt| { *state += evt; })
            .with_codec(
                |e: &i64| Ok(e.to_le_bytes().to_vec()),
                |b: &[u8]| {
                    let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
                    Ok(i64::from_le_bytes(arr))
                },
            );
        rp2.recover(journal.clone(), &permits).await.unwrap();
        assert_eq!(rp2.state(), &6);
    }

    #[tokio::test]
    async fn missing_codec_is_a_typed_error() {
        let journal = Arc::new(InMemoryJournal::default());
        let mut rp: ReceivePersistent<i64, i64, DummyErr> = ReceivePersistent::new("pid-2")
            .on_command(|_, c| Ok(vec![c]))
            .on_event(|s, e| { *s += e; });
        let r = rp.handle(journal, 1).await;
        assert!(matches!(r, Err(EventsourcedError::Codec(_))));
    }
}
