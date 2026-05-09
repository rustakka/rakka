//! `JournalOffsetStore` survives publisher restarts and uses a real
//! `Journal` backend (here `InMemoryJournal`) for durability.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::outbox::{JournalOffsetStore, OutboxOffsetStore, OutboxPattern};
use atomr_patterns::topology::Topology;
use atomr_persistence::{InMemoryJournal, Journal, PersistentRepr};
use atomr_persistence_query_inmemory::read_journal;

#[tokio::test]
async fn journal_offset_store_persists_across_restarts() {
    let system = ActorSystem::create("outbox-durable", Config::reference()).await.unwrap();
    // One journal hosts both the source events and the offset store —
    // a realistic single-backend deployment.
    let journal = Arc::new(InMemoryJournal::default());
    let rj = Arc::new(read_journal(journal.clone()));
    let store = Arc::new(JournalOffsetStore::new(journal.clone(), "demo").await);
    let published = Arc::new(AtomicU64::new(0));

    // Pre-write 4 events.
    for n in 1..=4u64 {
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

    let p1_handles = OutboxPattern::<u64>::builder()
        .read_journal(rj.clone())
        .poll_interval(Duration::from_millis(20))
        .offset_store(store.clone())
        .decode(|b: &[u8]| {
            Ok(u64::from_le_bytes(b.try_into().map_err(|_| "len".to_string())?))
        })
        .publish({
            let p = published.clone();
            move |_n: u64| {
                let p = p.clone();
                async move {
                    p.fetch_add(1, Ordering::AcqRel);
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
    while p1_handles.published() < 4 {
        if tokio::time::Instant::now() >= deadline {
            panic!("publisher 1 stuck at {}", p1_handles.published());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    p1_handles.stop();

    // Allow the publisher's offset save to flush to the journal
    // before we look at its persisted state.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Construct a fresh JournalOffsetStore from the same journal and
    // verify it loads the previously-saved offsets.
    let reconstructed = JournalOffsetStore::new(journal.clone(), "demo").await;
    let loaded = reconstructed.load();
    assert_eq!(loaded.get("agg").copied(), Some(4u64), "offset persisted to journal");

    // Add 2 more source events and run a fresh publisher with the
    // reconstructed store.
    for n in 5..=6u64 {
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

    let p2_handles = OutboxPattern::<u64>::builder()
        .read_journal(rj.clone())
        .poll_interval(Duration::from_millis(20))
        .offset_store(Arc::new(reconstructed))
        .decode(|b: &[u8]| {
            Ok(u64::from_le_bytes(b.try_into().map_err(|_| "len".to_string())?))
        })
        .publish({
            let p = published.clone();
            move |_n: u64| {
                let p = p.clone();
                async move {
                    p.fetch_add(1, Ordering::AcqRel);
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
    while p2_handles.published() < 2 {
        if tokio::time::Instant::now() >= deadline {
            panic!("publisher 2 stuck at {}", p2_handles.published());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        published.load(Ordering::Acquire),
        6,
        "exactly 6 publishes, no dupes"
    );
    p2_handles.stop();
    system.terminate().await;
}
