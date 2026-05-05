//! Eventsourced integration spec parity. akka.net:
//! `PersistentActorSpec`, `PersistentActorJournalProtocolSpec`,
//! `OptimizedRecoverySpec`, `ManyRecoveriesSpec`. Drives the trait
//! through scenarios the inline tests don't already cover.

use std::sync::Arc;

use async_trait::async_trait;
use atomr_persistence::{
    Eventsourced, EventsourcedError, InMemoryJournal, Journal, RecoveryPermitter,
};

#[derive(Debug, thiserror::Error, PartialEq)]
enum LedgerErr {
    #[error("rejected")]
    Rejected,
}

#[derive(Default)]
struct LedgerState {
    balance: i64,
    entries: u32,
}

enum LedgerCmd {
    Deposit(i64),
    /// Returns an empty events vec, exercising the no-events branch.
    Heartbeat,
    Reject,
    /// Splits into two events in one command, exercising multi-event persists.
    Pair(i64, i64),
}

#[derive(Clone)]
enum LedgerEvent {
    Adjusted(i64),
}

struct Ledger {
    id: String,
}

#[async_trait]
impl Eventsourced for Ledger {
    type Command = LedgerCmd;
    type Event = LedgerEvent;
    type State = LedgerState;
    type Error = LedgerErr;

    fn persistence_id(&self) -> String {
        self.id.clone()
    }

    fn command_to_events(
        &self,
        _state: &Self::State,
        cmd: Self::Command,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(match cmd {
            LedgerCmd::Deposit(n) => vec![LedgerEvent::Adjusted(n)],
            LedgerCmd::Heartbeat => Vec::new(),
            LedgerCmd::Reject => return Err(LedgerErr::Rejected),
            LedgerCmd::Pair(a, b) => vec![LedgerEvent::Adjusted(a), LedgerEvent::Adjusted(b)],
        })
    }

    fn apply_event(state: &mut Self::State, event: &Self::Event) {
        match event {
            LedgerEvent::Adjusted(n) => {
                state.balance += n;
                state.entries += 1;
            }
        }
    }

    fn encode_event(event: &Self::Event) -> Result<Vec<u8>, String> {
        let LedgerEvent::Adjusted(n) = event;
        Ok(n.to_le_bytes().to_vec())
    }

    fn decode_event(bytes: &[u8]) -> Result<Self::Event, String> {
        if bytes.len() != 8 {
            return Err(format!("bad len: {}", bytes.len()));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(LedgerEvent::Adjusted(i64::from_le_bytes(buf)))
    }
}

#[tokio::test]
async fn heartbeat_persists_no_events_and_does_not_advance_seq() {
    let journal = Arc::new(InMemoryJournal::default());
    let l = Ledger { id: "l-hb".into() };
    let mut state = LedgerState::default();
    let mut seq = 0u64;
    l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Heartbeat).await.unwrap();
    assert_eq!(seq, 0, "no events → seq stays at 0");
    assert_eq!(state.entries, 0);
    assert_eq!(journal.highest_sequence_nr("l-hb", 0).await.unwrap(), 0);
}

#[tokio::test]
async fn rejected_command_does_not_persist() {
    let journal = Arc::new(InMemoryJournal::default());
    let l = Ledger { id: "l-reject".into() };
    let mut state = LedgerState::default();
    let mut seq = 0u64;
    let r = l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Reject).await;
    assert!(matches!(r, Err(EventsourcedError::Domain(LedgerErr::Rejected))));
    assert_eq!(seq, 0);
    assert_eq!(state.balance, 0);
}

#[tokio::test]
async fn multi_event_command_advances_seq_by_n() {
    let journal = Arc::new(InMemoryJournal::default());
    let l = Ledger { id: "l-pair".into() };
    let mut state = LedgerState::default();
    let mut seq = 0u64;
    l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Pair(3, 4)).await.unwrap();
    assert_eq!(seq, 2, "pair command persists 2 events");
    assert_eq!(state.balance, 7);
    assert_eq!(state.entries, 2);
    assert_eq!(journal.highest_sequence_nr("l-pair", 0).await.unwrap(), 2);
}

#[tokio::test]
async fn empty_journal_recover_yields_default_state() {
    let journal = Arc::new(InMemoryJournal::default());
    let permitter = RecoveryPermitter::new(1);
    let mut l = Ledger { id: "l-empty".into() };
    let mut state = LedgerState::default();
    let h = l.recover(journal, &mut state, &permitter).await.unwrap();
    assert_eq!(h, 0);
    assert_eq!(state.balance, 0);
    assert_eq!(state.entries, 0);
}

#[tokio::test]
async fn replay_after_multi_event_command_restores_full_state() {
    let journal = Arc::new(InMemoryJournal::default());
    let permitter = RecoveryPermitter::new(1);

    let l = Ledger { id: "l-replay".into() };
    let mut state = LedgerState::default();
    let mut seq = 0u64;
    l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Deposit(10)).await.unwrap();
    l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Pair(1, 2)).await.unwrap();
    l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Heartbeat).await.unwrap();

    let mut l2 = Ledger { id: "l-replay".into() };
    let mut state2 = LedgerState::default();
    let h = l2.recover(journal, &mut state2, &permitter).await.unwrap();
    assert_eq!(h, 3);
    assert_eq!(state2.balance, 13);
    assert_eq!(state2.entries, 3);
}

#[tokio::test]
async fn recovery_permitter_caps_concurrent_recoveries() {
    let journal = Arc::new(InMemoryJournal::default());
    // Pre-populate so recovery has something to do.
    let l = Ledger { id: "l-cap".into() };
    let mut state = LedgerState::default();
    let mut seq = 0u64;
    for i in 0..3 {
        l.handle_command(journal.clone(), &mut state, &mut seq, "w", LedgerCmd::Deposit(i + 1))
            .await
            .unwrap();
    }
    // Single-permit permitter; close it to deny all incoming recovers.
    let permitter = RecoveryPermitter::new(1);
    permitter.close();
    let mut l2 = Ledger { id: "l-cap".into() };
    let mut state2 = LedgerState::default();
    let r = l2.recover(journal, &mut state2, &permitter).await;
    assert!(matches!(r, Err(EventsourcedError::PermitDenied)));
}
