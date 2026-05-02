//! Phase 14.C example — `Eventsourced` counter with periodic
//! snapshots and `RecoveryPermitter`-bounded recovery.
//!
//! Demonstrates:
//! * `Eventsourced` trait with typed `Error`.
//! * `RecoveryPermitter` capping concurrent replays.
//! * `AsyncSnapshotter` with a `Periodic { every: 10 }` policy.
//!
//! Run with `cargo run -p example-event-sourced-counter`.

use std::sync::Arc;

use async_trait::async_trait;
use rakka_persistence::{
    AsyncSnapshotter, Eventsourced, EventsourcedError, InMemoryJournal, InMemorySnapshotStore,
    RecoveryPermitter, SnapshotPolicy,
};

#[derive(Debug, thiserror::Error)]
#[error("counter error: {0}")]
struct CounterErr(String);

#[derive(Default, Debug)]
struct CounterState {
    n: i64,
}

#[derive(Clone, Debug)]
enum CounterEvent {
    Adjusted(i64),
}

enum CounterCmd {
    Add(i64),
    #[allow(dead_code)]
    Sub(i64),
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
            return Err(CounterErr("would underflow".into()));
        }
        Ok(vec![CounterEvent::Adjusted(delta)])
    }

    fn apply_event(state: &mut Self::State, e: &Self::Event) {
        match e {
            CounterEvent::Adjusted(d) => state.n += d,
        }
    }

    fn encode_event(e: &Self::Event) -> Result<Vec<u8>, String> {
        match e {
            CounterEvent::Adjusted(d) => Ok(d.to_le_bytes().to_vec()),
        }
    }

    fn decode_event(bytes: &[u8]) -> Result<Self::Event, String> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| "len".to_string())?;
        Ok(CounterEvent::Adjusted(i64::from_le_bytes(arr)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let journal = Arc::new(InMemoryJournal::default());
    let snapshots = InMemorySnapshotStore::new();
    let permits = RecoveryPermitter::new(4);
    let snapshotter = AsyncSnapshotter::new(snapshots.clone(), SnapshotPolicy::Periodic { every: 10 });

    // Boot 3 actors, run 25 commands each.
    let mut state = CounterState::default();
    let mut seq = 0u64;
    let counter = Counter { id: "demo-1".into() };

    for i in 1..=25i64 {
        counter
            .handle_command(journal.clone(), &mut state, &mut seq, "writer", CounterCmd::Add(i))
            .await
            .map_err(|e: EventsourcedError<CounterErr>| anyhow::anyhow!("{}", e))?;
        if snapshotter.should_snapshot(seq) {
            // payload: just write the running total
            snapshotter.save(counter.persistence_id(), seq, state.n.to_le_bytes().to_vec()).await;
            println!("snapshot saved @ seq={seq}, value={}", state.n);
        }
    }
    println!("post-write state.n = {}", state.n);

    // Replay-on-restart.
    let mut counter2 = Counter { id: "demo-1".into() };
    let mut state2 = CounterState::default();
    let highest = counter2
        .recover(journal.clone(), &mut state2, &permits)
        .await
        .map_err(|e: EventsourcedError<CounterErr>| anyhow::anyhow!("{}", e))?;
    println!("recovered through seq={highest}, state.n = {}", state2.n);
    assert_eq!(state.n, state2.n);
    Ok(())
}
