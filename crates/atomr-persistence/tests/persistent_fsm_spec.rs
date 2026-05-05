//! `PersistentFSM` spec parity. akka.net: `PersistentFSMSpec`.
//!
//! Asserts the public-API invariants of [`atomr_persistence::PersistentFSM`]:
//!
//! * Every event returned from the command handler is appended to the journal,
//!   in handler-emit order, with monotonic sequence numbers.
//! * A synthetic restart (re-construct the FSM with the same `persistence_id`)
//!   recovers the exact final `(state, data)` pair from the journal.
//! * Recovery against an empty journal leaves `initial_state` / `initial_data`
//!   untouched.
//! * Events that mutate `S` (state) and events that mutate `D` (data) both
//!   replay correctly, regardless of the order in which they were emitted.
//! * Recovery is gated by [`RecoveryPermitter`]: while the permitter is closed
//!   no recovery proceeds (returns `EventsourcedError::PermitDenied`).

use std::sync::Arc;
use std::time::Duration;

use atomr_persistence::{
    EventsourcedError, InMemoryJournal, Journal, PersistentFSM, RecoveryPermitter,
};

// ---------------------------------------------------------------------------
// Counter FSM domain — exercises both state-mutation and data-mutation events.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum CounterState {
    Idle,
    Running,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct CounterData {
    n: i64,
}

#[derive(Clone, Debug)]
enum CounterCmd {
    Start,
    Stop,
    Tick(i64),
}

#[derive(Clone, Debug, PartialEq)]
enum CounterEvent {
    Started,         // mutates S: Idle -> Running
    Stopped,         // mutates S: Running -> Idle
    Ticked(i64),     // mutates D: n += delta
}

#[derive(Debug, thiserror::Error)]
#[error("not running")]
struct CounterErr;

fn encode(e: &CounterEvent) -> Result<Vec<u8>, String> {
    Ok(match e {
        CounterEvent::Started => vec![0u8],
        CounterEvent::Stopped => vec![1u8],
        CounterEvent::Ticked(d) => {
            let mut v = Vec::with_capacity(9);
            v.push(2u8);
            v.extend_from_slice(&d.to_le_bytes());
            v
        }
    })
}

fn decode(bytes: &[u8]) -> Result<CounterEvent, String> {
    match bytes.first() {
        Some(0) => Ok(CounterEvent::Started),
        Some(1) => Ok(CounterEvent::Stopped),
        Some(2) if bytes.len() == 9 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[1..9]);
            Ok(CounterEvent::Ticked(i64::from_le_bytes(buf)))
        }
        _ => Err(format!("bad bytes: {bytes:?}")),
    }
}

fn make_fsm(id: &str) -> PersistentFSM<CounterState, CounterData, CounterCmd, CounterEvent, CounterErr> {
    PersistentFSM::new(id)
        .with_initial(CounterState::Idle, CounterData::default())
        .on_command(|s, _d, c| match (s, c) {
            (CounterState::Idle, CounterCmd::Start) => Ok(vec![CounterEvent::Started]),
            (CounterState::Running, CounterCmd::Stop) => Ok(vec![CounterEvent::Stopped]),
            (CounterState::Running, CounterCmd::Tick(d)) => Ok(vec![CounterEvent::Ticked(d)]),
            // Domain rejection — must NOT mutate state or write events.
            _ => Err(CounterErr),
        })
        .on_event(|s, d, evt| match evt {
            CounterEvent::Started => *s = CounterState::Running,
            CounterEvent::Stopped => *s = CounterState::Idle,
            CounterEvent::Ticked(delta) => d.n += delta,
        })
        .with_codec(encode, decode)
}

// ---------------------------------------------------------------------------
// Spec assertions
// ---------------------------------------------------------------------------

/// Every event the command handler returns is appended to the journal in
/// emit-order with monotonic sequence numbers starting at 1.
#[tokio::test]
async fn events_are_recorded_in_order() {
    let journal = Arc::new(InMemoryJournal::default());
    let mut fsm = make_fsm("counter-record");

    fsm.handle(journal.clone(), CounterCmd::Start).await.unwrap();
    fsm.handle(journal.clone(), CounterCmd::Tick(3)).await.unwrap();
    fsm.handle(journal.clone(), CounterCmd::Tick(4)).await.unwrap();
    fsm.handle(journal.clone(), CounterCmd::Stop).await.unwrap();

    let highest = journal.highest_sequence_nr("counter-record", 0).await.unwrap();
    assert_eq!(highest, 4, "one event per accepted command");

    let reprs = journal.replay_messages("counter-record", 1, highest, u64::MAX).await.unwrap();
    let decoded: Vec<CounterEvent> =
        reprs.iter().map(|r| decode(&r.payload).unwrap()).collect();
    assert_eq!(
        decoded,
        vec![
            CounterEvent::Started,
            CounterEvent::Ticked(3),
            CounterEvent::Ticked(4),
            CounterEvent::Stopped,
        ]
    );
    // Sequence numbers are monotonic and contiguous from 1.
    let seqs: Vec<u64> = reprs.iter().map(|r| r.sequence_nr).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
}

/// After a synthetic restart with the same `persistence_id`, the new FSM
/// recovers identical state and data from the journal.
#[tokio::test]
async fn restart_recovers_state_and_data() {
    let journal = Arc::new(InMemoryJournal::default());
    let permits = RecoveryPermitter::new(1);

    let mut fsm = make_fsm("counter-restart");
    fsm.handle(journal.clone(), CounterCmd::Start).await.unwrap();
    fsm.handle(journal.clone(), CounterCmd::Tick(10)).await.unwrap();
    fsm.handle(journal.clone(), CounterCmd::Tick(-3)).await.unwrap();
    assert_eq!(fsm.state(), Some(&CounterState::Running));
    assert_eq!(fsm.data(), Some(&CounterData { n: 7 }));

    // Synthetic restart: brand-new FSM, same persistence_id, same journal.
    let mut fsm2 = make_fsm("counter-restart");
    let highest = fsm2.recover(journal.clone(), &permits).await.unwrap();
    assert_eq!(highest, 3);
    assert_eq!(fsm2.state(), Some(&CounterState::Running));
    assert_eq!(fsm2.data(), Some(&CounterData { n: 7 }));
}

/// Recovery against an empty journal yields the initial `(state, data)`
/// pair untouched and reports `highest_seq = 0`.
#[tokio::test]
async fn empty_journal_recovery_yields_initial() {
    let journal = Arc::new(InMemoryJournal::default());
    let permits = RecoveryPermitter::new(1);

    let mut fsm = make_fsm("counter-empty");
    let highest = fsm.recover(journal, &permits).await.unwrap();

    assert_eq!(highest, 0);
    assert_eq!(fsm.state(), Some(&CounterState::Idle));
    assert_eq!(fsm.data(), Some(&CounterData { n: 0 }));
    assert!(fsm.transitions().is_empty());
}

/// State-mutating events (Started/Stopped) and data-mutating events (Ticked)
/// both replay correctly when interleaved.
#[tokio::test]
async fn interleaved_state_and_data_events_replay() {
    let journal = Arc::new(InMemoryJournal::default());
    let permits = RecoveryPermitter::new(1);

    let mut fsm = make_fsm("counter-mixed");
    fsm.handle(journal.clone(), CounterCmd::Start).await.unwrap();      // S mutate
    fsm.handle(journal.clone(), CounterCmd::Tick(5)).await.unwrap();    // D mutate
    fsm.handle(journal.clone(), CounterCmd::Stop).await.unwrap();       // S mutate
    // Domain reject — no event should land in the journal.
    let r = fsm.handle(journal.clone(), CounterCmd::Tick(99)).await;
    assert!(matches!(r, Err(EventsourcedError::Domain(CounterErr))));
    fsm.handle(journal.clone(), CounterCmd::Start).await.unwrap();      // S mutate
    fsm.handle(journal.clone(), CounterCmd::Tick(-2)).await.unwrap();   // D mutate

    assert_eq!(fsm.state(), Some(&CounterState::Running));
    assert_eq!(fsm.data(), Some(&CounterData { n: 3 }));

    // Exactly 5 events on disk — the rejected Tick(99) was NOT persisted.
    let highest = journal.highest_sequence_nr("counter-mixed", 0).await.unwrap();
    assert_eq!(highest, 5);

    // Replay reproduces both axes of mutation.
    let mut fsm2 = make_fsm("counter-mixed");
    fsm2.recover(journal, &permits).await.unwrap();
    assert_eq!(fsm2.state(), Some(&CounterState::Running));
    assert_eq!(fsm2.data(), Some(&CounterData { n: 3 }));
    // 3 boundary crossings: Idle->Running, Running->Idle, Idle->Running.
    assert_eq!(fsm2.transitions().len(), 3);
}

/// While the `RecoveryPermitter` is closed, recovery does not proceed —
/// `acquire` returns `None` and the driver yields `PermitDenied`.
#[tokio::test]
async fn closed_permitter_blocks_recovery() {
    let journal = Arc::new(InMemoryJournal::default());
    // Pre-populate the journal so we can prove no replay happened.
    let mut writer = make_fsm("counter-permit");
    writer.handle(journal.clone(), CounterCmd::Start).await.unwrap();
    writer.handle(journal.clone(), CounterCmd::Tick(42)).await.unwrap();

    let permits = RecoveryPermitter::new(1);
    permits.close();

    let mut fsm = make_fsm("counter-permit");
    let r = fsm.recover(journal.clone(), &permits).await;
    assert!(
        matches!(r, Err(EventsourcedError::PermitDenied)),
        "closed permitter must yield PermitDenied, got {r:?}"
    );
    // No replay happened — state stayed at the initial values.
    assert_eq!(fsm.state(), Some(&CounterState::Idle));
    assert_eq!(fsm.data(), Some(&CounterData { n: 0 }));
}

/// While the permitter has zero free slots, recovery blocks rather than
/// proceeding. Releasing the held permit unblocks it.
#[tokio::test]
async fn busy_permitter_blocks_until_released() {
    let journal = Arc::new(InMemoryJournal::default());
    let mut writer = make_fsm("counter-busy");
    writer.handle(journal.clone(), CounterCmd::Start).await.unwrap();
    writer.handle(journal.clone(), CounterCmd::Tick(7)).await.unwrap();

    let permits = RecoveryPermitter::new(1);
    let held = permits.try_acquire().expect("first permit available");

    let permits_for_task = permits.clone();
    let journal_for_task = journal.clone();
    let h = tokio::spawn(async move {
        let mut fsm = make_fsm("counter-busy");
        fsm.recover(journal_for_task, &permits_for_task).await.map(|_| fsm)
    });

    // Give the task a chance to attempt acquire; it must still be pending.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!h.is_finished(), "recovery must wait while permitter is busy");

    drop(held);
    let fsm = tokio::time::timeout(Duration::from_secs(1), h)
        .await
        .expect("recovery completes within timeout")
        .expect("task did not panic")
        .expect("recover succeeded");
    assert_eq!(fsm.state(), Some(&CounterState::Running));
    assert_eq!(fsm.data(), Some(&CounterData { n: 7 }));
}
