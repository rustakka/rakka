//! Outbox: persisted events are republished exactly once across a
//! publisher restart.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::outbox::{InMemoryOffsetStore, OutboxPattern};
use atomr_patterns::topology::Topology;
use atomr_persistence::{InMemoryJournal, Journal, PersistentRepr};
use atomr_persistence_query_inmemory::read_journal;

#[tokio::test]
async fn publisher_resumes_from_offset_store_after_restart() {
    let system = ActorSystem::create("outbox", Config::reference()).await.unwrap();
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));
    let store = Arc::new(InMemoryOffsetStore::new());

    let published_total = Arc::new(AtomicU64::new(0));
    let published_clone = published_total.clone();

    // Pre-write 5 events.
    for n in 1..=5u64 {
        journal
            .write_messages(vec![PersistentRepr {
                persistence_id: "agg".into(),
                sequence_nr: n,
                payload: n.to_le_bytes().to_vec(),
                manifest: "evt".into(),
                writer_uuid: "w".into(),
                deleted: false,
                tags: vec![],
            }])
            .await
            .unwrap();
    }

    // First publisher run.
    let p1_handles = OutboxPattern::<u64>::builder()
        .read_journal(rj.clone())
        .poll_interval(Duration::from_millis(20))
        .offset_store(store.clone())
        .decode(|b: &[u8]| {
            let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
            Ok(u64::from_le_bytes(arr))
        })
        .publish({
            let pc = published_clone.clone();
            move |_n: u64| {
                let pc = pc.clone();
                async move {
                    pc.fetch_add(1, Ordering::AcqRel);
                    true
                }
            }
        })
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();

    // Wait until 5 published.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while p1_handles.published() < 5 {
        if tokio::time::Instant::now() >= deadline {
            panic!("publisher never reached 5: {}", p1_handles.published());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    p1_handles.stop();

    // Add 3 more events after stopping the publisher.
    for n in 6..=8u64 {
        journal
            .write_messages(vec![PersistentRepr {
                persistence_id: "agg".into(),
                sequence_nr: n,
                payload: n.to_le_bytes().to_vec(),
                manifest: "evt".into(),
                writer_uuid: "w".into(),
                deleted: false,
                tags: vec![],
            }])
            .await
            .unwrap();
    }

    // Second publisher run reuses the same offset store; should only
    // publish the 3 new events.
    let p2_handles = OutboxPattern::<u64>::builder()
        .read_journal(rj.clone())
        .poll_interval(Duration::from_millis(20))
        .offset_store(store.clone())
        .decode(|b: &[u8]| {
            let arr: [u8; 8] = b.try_into().map_err(|_| "len".to_string())?;
            Ok(u64::from_le_bytes(arr))
        })
        .publish({
            let pc = published_total.clone();
            move |_n: u64| {
                let pc = pc.clone();
                async move {
                    pc.fetch_add(1, Ordering::AcqRel);
                    true
                }
            }
        })
        .build()
        .unwrap()
        .materialize(&system)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while p2_handles.published() < 3 {
        if tokio::time::Instant::now() >= deadline {
            panic!("publisher 2 never reached 3 new: {}", p2_handles.published());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Total across both runs should be exactly 8 (no double-publishes).
    assert_eq!(published_total.load(Ordering::Acquire), 8, "exactly 8 publishes total, none repeated");

    p2_handles.stop();
    system.terminate().await;
}
