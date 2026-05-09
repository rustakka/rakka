//! Snapshot-first recovery: a periodic snapshot policy fires; on
//! restart the gateway loads the snapshot and only replays events
//! after the snapshot offset.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::prelude::*;
use atomr_persistence::{Eventsourced, InMemoryJournal, InMemorySnapshotStore, SnapshotPolicy};

// Global decode counters — let the test assert that snapshot-first
// recovery actually skipped events.
static EVENT_DECODES: AtomicU64 = AtomicU64::new(0);
static STATE_DECODES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
#[error("counter err")]
struct E;

#[derive(Default, Debug, Clone)]
struct Counter {
    n: i64,
}

#[derive(Clone, Debug)]
struct Tick(i64);

impl DomainEvent for Tick {}

#[derive(Debug)]
struct Add(i64);

impl Command for Add {
    type AggregateId = String;
    fn aggregate_id(&self) -> String {
        "the-counter".into()
    }
}

struct Aggregate {
    id: String,
}

#[async_trait]
impl Eventsourced for Aggregate {
    type Command = Add;
    type Event = Tick;
    type State = Counter;
    type Error = E;

    fn persistence_id(&self) -> String {
        self.id.clone()
    }

    fn command_to_events(&self, _: &Counter, cmd: Add) -> Result<Vec<Tick>, E> {
        Ok(vec![Tick(cmd.0)])
    }
    fn apply_event(state: &mut Counter, e: &Tick) {
        state.n += e.0;
    }
    fn encode_event(e: &Tick) -> Result<Vec<u8>, String> {
        Ok(e.0.to_le_bytes().to_vec())
    }
    fn decode_event(b: &[u8]) -> Result<Tick, String> {
        EVENT_DECODES.fetch_add(1, Ordering::SeqCst);
        let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
        Ok(Tick(i64::from_le_bytes(arr)))
    }
}

impl AggregateRoot for Aggregate {
    type Id = String;
    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn encode_state(state: &Counter) -> Option<Result<Vec<u8>, String>> {
        Some(Ok(state.n.to_le_bytes().to_vec()))
    }

    fn decode_state(b: &[u8]) -> Result<Counter, String> {
        STATE_DECODES.fetch_add(1, Ordering::SeqCst);
        let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
        Ok(Counter { n: i64::from_le_bytes(arr) })
    }
}

// Serialized into one test fn because the static decode counters are
// shared across the whole process and parallel test execution would
// interleave them.

async fn case_snapshot_first_recovery_skips_replayed_events() {
    EVENT_DECODES.store(0, Ordering::SeqCst);
    STATE_DECODES.store(0, Ordering::SeqCst);

    let journal = Arc::new(InMemoryJournal::default());
    let snap_store: Arc<InMemorySnapshotStore> = InMemorySnapshotStore::new();

    // First incarnation: 25 commands, snapshot every 10.
    {
        let system = ActorSystem::create("snap-1", Config::reference()).await.unwrap();
        let topology = CqrsPattern::<Aggregate>::builder(journal.clone())
            .factory(|id| Aggregate { id })
            .snapshot_store(snap_store.clone())
            .snapshot_policy(SnapshotPolicy::Periodic { every: 10 })
            .build()
            .unwrap();
        let h = topology.materialize(&system).await.unwrap();
        let repo = h.repository();
        for i in 1..=25i64 {
            repo.send(Add(i)).await.unwrap();
        }
        system.terminate().await;
    }

    // 25 events were applied during normal handling (via apply_event,
    // not decode). decode_event is only called during journal replay
    // recovery — first incarnation never recovered, so 0 decodes.
    assert_eq!(EVENT_DECODES.load(Ordering::SeqCst), 0, "no replay on first run");

    // Snapshot store should have at least one entry at seq=20 (last
    // multiple of 10 below 25).
    let loaded = snap_store.load("the-counter").await;
    let (meta, _payload) = loaded.expect("snapshot present");
    assert_eq!(meta.sequence_nr, 20);

    // Second incarnation: send one more command. Recovery should load
    // snapshot (seq=20), then replay only events 21..=25.
    let final_state = {
        let system = ActorSystem::create("snap-2", Config::reference()).await.unwrap();
        let topology = CqrsPattern::<Aggregate>::builder(journal.clone())
            .factory(|id| Aggregate { id })
            .snapshot_store(snap_store.clone())
            .snapshot_policy(SnapshotPolicy::Periodic { every: 10 })
            .build()
            .unwrap();
        let h = topology.materialize(&system).await.unwrap();
        let repo = h.repository();
        repo.send(Add(100)).await.unwrap();
        system.terminate().await;
        // Total expected: 1+2+...+25 + 100 = 325 + 100 = 425.
        425i64
    };
    let _ = final_state;

    // Snapshot loaded once, exactly 5 events replayed (21..=25), not 25.
    assert_eq!(STATE_DECODES.load(Ordering::SeqCst), 1, "snapshot decoded once");
    assert_eq!(
        EVENT_DECODES.load(Ordering::SeqCst),
        5,
        "only events after the snapshot are replayed"
    );
}

async fn case_no_snapshot_store_falls_back_to_full_replay() {
    EVENT_DECODES.store(0, Ordering::SeqCst);
    let journal = Arc::new(InMemoryJournal::default());

    {
        let system = ActorSystem::create("nosnap-1", Config::reference()).await.unwrap();
        let topology = CqrsPattern::<Aggregate>::builder(journal.clone())
            .factory(|id| Aggregate { id })
            .build()
            .unwrap();
        let h = topology.materialize(&system).await.unwrap();
        let repo = h.repository();
        for i in 1..=10i64 {
            repo.send(Add(i)).await.unwrap();
        }
        system.terminate().await;
    }

    {
        let system = ActorSystem::create("nosnap-2", Config::reference()).await.unwrap();
        let topology = CqrsPattern::<Aggregate>::builder(journal.clone())
            .factory(|id| Aggregate { id })
            .build()
            .unwrap();
        let h = topology.materialize(&system).await.unwrap();
        let repo = h.repository();
        repo.send(Add(99)).await.unwrap();
        system.terminate().await;
    }

    assert_eq!(EVENT_DECODES.load(Ordering::SeqCst), 10, "all 10 events replayed");
}

#[tokio::test]
async fn snapshot_recovery_cases() {
    case_snapshot_first_recovery_skips_replayed_events().await;
    case_no_snapshot_store_falls_back_to_full_replay().await;
}
