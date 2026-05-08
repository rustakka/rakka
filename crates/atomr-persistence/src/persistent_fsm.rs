//! `PersistentFSM` — event-sourced state machine on top of [`Eventsourced`].
//!
//! Two-shape model:
//!
//! * `S` — finite state (typically an enum: `Idle`, `Active`, …).
//! * `D` — state-data carried alongside `S`.
//!
//! The actor receives commands, the registered transition function
//! decides `(next_state, persisted_event)` per command, and recovery
//! re-applies events to rebuild the `(S, D)` pair.
//!
//! ```text
//! let fsm = PersistentFSM::<DoorState, DoorData, DoorCmd, DoorEvent>::new("door-1")
//!     .with_initial(DoorState::Closed, DoorData::default())
//!     .on_command(|state, data, cmd| { … })
//!     .on_event(|state, data, evt| { … });
//! ```

use std::sync::Arc;

use crate::eventsourced::EventsourcedError;
use crate::journal::{Journal, PersistentRepr};
use crate::recovery_permitter::RecoveryPermitter;

type CmdFn<S, D, C, E, Err> = Box<dyn FnMut(&S, &D, C) -> Result<Vec<E>, Err> + Send + 'static>;
type EvtFn<S, D, E> = Box<dyn FnMut(&mut S, &mut D, &E) + Send + 'static>;
type EncodeFn<E> = Box<dyn Fn(&E) -> Result<Vec<u8>, String> + Send + Sync>;
type DecodeFn<E> = Box<dyn Fn(&[u8]) -> Result<E, String> + Send + Sync>;

pub struct PersistentFSM<S, D, C, E, Err>
where
    S: Clone + Send + 'static,
    D: Send + 'static,
    C: Send + 'static,
    E: Clone + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    persistence_id: String,
    state: Option<S>,
    data: Option<D>,
    next_seq: u64,
    on_command: Option<CmdFn<S, D, C, E, Err>>,
    on_event: Option<EvtFn<S, D, E>>,
    encode: Option<EncodeFn<E>>,
    decode: Option<DecodeFn<E>>,
    transitions: Vec<(S, S)>,
}

impl<S, D, C, E, Err> PersistentFSM<S, D, C, E, Err>
where
    S: Clone + PartialEq + std::fmt::Debug + Send + 'static,
    D: Send + 'static,
    C: Send + 'static,
    E: Clone + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    pub fn new(persistence_id: impl Into<String>) -> Self {
        Self {
            persistence_id: persistence_id.into(),
            state: None,
            data: None,
            next_seq: 0,
            on_command: None,
            on_event: None,
            encode: None,
            decode: None,
            transitions: Vec::new(),
        }
    }

    pub fn with_initial(mut self, s: S, d: D) -> Self {
        self.state = Some(s);
        self.data = Some(d);
        self
    }

    pub fn on_command<F>(mut self, f: F) -> Self
    where
        F: FnMut(&S, &D, C) -> Result<Vec<E>, Err> + Send + 'static,
    {
        self.on_command = Some(Box::new(f));
        self
    }

    pub fn on_event<F>(mut self, f: F) -> Self
    where
        F: FnMut(&mut S, &mut D, &E) + Send + 'static,
    {
        self.on_event = Some(Box::new(f));
        self
    }

    pub fn with_codec<EncF, DecF>(mut self, encode: EncF, decode: DecF) -> Self
    where
        EncF: Fn(&E) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        DecF: Fn(&[u8]) -> Result<E, String> + Send + Sync + 'static,
    {
        self.encode = Some(Box::new(encode));
        self.decode = Some(Box::new(decode));
        self
    }

    pub fn state(&self) -> Option<&S> {
        self.state.as_ref()
    }

    pub fn data(&self) -> Option<&D> {
        self.data.as_ref()
    }

    /// History of state transitions seen since boot. Useful for tests.
    pub fn transitions(&self) -> &[(S, S)] {
        &self.transitions
    }

    pub async fn recover<J: Journal>(
        &mut self,
        journal: Arc<J>,
        permitter: &RecoveryPermitter,
    ) -> Result<u64, EventsourcedError<Err>> {
        let _permit = permitter.acquire().await.ok_or(EventsourcedError::PermitDenied)?;
        let on_event = self
            .on_event
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_event not registered".into()))?;
        let decode =
            self.decode.as_ref().ok_or_else(|| EventsourcedError::Codec("decoder not registered".into()))?;
        let highest = journal.highest_sequence_nr(&self.persistence_id, 0).await?;
        let events = journal.replay_messages(&self.persistence_id, 1, highest, u64::MAX).await?;
        for e in &events {
            let evt = decode(&e.payload).map_err(EventsourcedError::Codec)?;
            let prev = self.state.clone();
            let (s, d) = (
                self.state.as_mut().expect("initial state required before recover"),
                self.data.as_mut().expect("initial data required before recover"),
            );
            on_event(s, d, &evt);
            if let (Some(p), Some(now)) = (prev, self.state.as_ref()) {
                if &p != now {
                    self.transitions.push((p, now.clone()));
                }
            }
        }
        self.next_seq = highest;
        Ok(highest)
    }

    pub async fn handle<J: Journal>(
        &mut self,
        journal: Arc<J>,
        cmd: C,
    ) -> Result<(), EventsourcedError<Err>> {
        let on_cmd = self
            .on_command
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_command not registered".into()))?;
        let on_event = self
            .on_event
            .as_mut()
            .ok_or_else(|| EventsourcedError::Codec("on_event not registered".into()))?;
        let encode =
            self.encode.as_ref().ok_or_else(|| EventsourcedError::Codec("encoder not registered".into()))?;
        let s =
            self.state.as_ref().ok_or_else(|| EventsourcedError::Codec("initial state not set".into()))?;
        let d = self.data.as_ref().ok_or_else(|| EventsourcedError::Codec("initial data not set".into()))?;
        let events = on_cmd(s, d, cmd).map_err(EventsourcedError::Domain)?;
        if events.is_empty() {
            return Ok(());
        }
        let mut reprs = Vec::with_capacity(events.len());
        for e in &events {
            self.next_seq += 1;
            let payload = encode(e).map_err(EventsourcedError::Codec)?;
            reprs.push(PersistentRepr {
                persistence_id: self.persistence_id.clone(),
                sequence_nr: self.next_seq,
                payload,
                manifest: "fsm".into(),
                writer_uuid: "fsm".into(),
                deleted: false,
                tags: Vec::new(),
            });
        }
        journal.write_messages(reprs).await?;
        for e in &events {
            let prev = self.state.clone();
            let s_mut = self.state.as_mut().expect("state present");
            let d_mut = self.data.as_mut().expect("data present");
            on_event(s_mut, d_mut, e);
            if let (Some(p), Some(now)) = (prev, self.state.as_ref()) {
                if &p != now {
                    self.transitions.push((p, now.clone()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryJournal;

    #[derive(Clone, Debug, PartialEq)]
    enum DoorState {
        Closed,
        Open,
    }

    #[derive(Default)]
    struct DoorData {
        opens: u32,
    }

    #[derive(Clone, Debug)]
    enum DoorCmd {
        Toggle,
    }

    #[derive(Clone, Debug)]
    enum DoorEvent {
        Toggled,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("dummy")]
    struct E;

    fn make_fsm(id: &str) -> PersistentFSM<DoorState, DoorData, DoorCmd, DoorEvent, E> {
        PersistentFSM::new(id)
            .with_initial(DoorState::Closed, DoorData::default())
            .on_command(|_s, _d, _c: DoorCmd| Ok(vec![DoorEvent::Toggled]))
            .on_event(|s, d, _evt: &DoorEvent| match s {
                DoorState::Closed => {
                    *s = DoorState::Open;
                    d.opens += 1;
                }
                DoorState::Open => {
                    *s = DoorState::Closed;
                }
            })
            .with_codec(|_| Ok(vec![0u8]), |_| Ok(DoorEvent::Toggled))
    }

    #[tokio::test]
    async fn fsm_transitions_and_recovers() {
        let journal = Arc::new(InMemoryJournal::default());
        let permits = RecoveryPermitter::new(1);

        let mut fsm = make_fsm("door-1");
        fsm.handle(journal.clone(), DoorCmd::Toggle).await.unwrap();
        fsm.handle(journal.clone(), DoorCmd::Toggle).await.unwrap();
        fsm.handle(journal.clone(), DoorCmd::Toggle).await.unwrap();
        assert_eq!(fsm.state(), Some(&DoorState::Open));
        assert_eq!(fsm.data().unwrap().opens, 2);
        assert_eq!(fsm.transitions().len(), 3);

        // Replay -> same final state.
        let mut fsm2 = make_fsm("door-1");
        fsm2.recover(journal.clone(), &permits).await.unwrap();
        assert_eq!(fsm2.state(), Some(&DoorState::Open));
        assert_eq!(fsm2.data().unwrap().opens, 2);
    }

    #[tokio::test]
    async fn missing_initial_state_is_typed_error() {
        let journal = Arc::new(InMemoryJournal::default());
        let mut fsm: PersistentFSM<DoorState, DoorData, DoorCmd, DoorEvent, E> = PersistentFSM::new("door-2")
            .on_command(|_, _, _| Ok(vec![DoorEvent::Toggled]))
            .on_event(|_, _, _| {})
            .with_codec(|_| Ok(vec![]), |_| Ok(DoorEvent::Toggled));
        let r = fsm.handle(journal, DoorCmd::Toggle).await;
        assert!(matches!(r, Err(EventsourcedError::Codec(_))));
    }
}
