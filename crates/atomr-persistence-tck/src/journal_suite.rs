//! Journal conformance suite. Every provider crate runs this against its
//! backend to validate semantic parity with the in-memory reference impl.

use std::sync::Arc;

use atomr_persistence::{Journal, JournalError, PersistentRepr};

fn repr(pid: &str, nr: u64, tag: Option<&str>) -> PersistentRepr {
    PersistentRepr {
        persistence_id: pid.into(),
        sequence_nr: nr,
        payload: vec![nr as u8],
        manifest: "m".into(),
        writer_uuid: "tck".into(),
        deleted: false,
        tags: tag.map(|t| vec![t.into()]).unwrap_or_default(),
    }
}

/// Simple write/replay round trip. Returns true on success.
pub async fn journal_round_trip<J: Journal>(journal: Arc<J>, pid: &str) -> bool {
    let batch: Vec<_> = (1..=5u64).map(|i| repr(pid, i, None)).collect();
    journal.write_messages(batch).await.unwrap();
    let replay = journal.replay_messages(pid, 1, 5, 100).await.unwrap();
    replay.len() == 5 && journal.highest_sequence_nr(pid, 0).await.unwrap() == 5
}

/// Full journal conformance suite. Panics with a descriptive message on
/// the first failing assertion so callers can wire it straight into
/// `#[tokio::test]` without additional boilerplate.
pub async fn journal_suite<J: Journal>(journal: Arc<J>, pid_prefix: &str) {
    let a = format!("{pid_prefix}-a");
    let b = format!("{pid_prefix}-b");

    journal.write_messages((1..=3u64).map(|i| repr(&a, i, None)).collect()).await.unwrap();
    journal.write_messages((1..=2u64).map(|i| repr(&b, i, None)).collect()).await.unwrap();

    let replay_a = journal.replay_messages(&a, 1, u64::MAX, 10).await.unwrap();
    assert_eq!(replay_a.len(), 3, "replay_a len");
    let replay_b = journal.replay_messages(&b, 1, u64::MAX, 10).await.unwrap();
    assert_eq!(replay_b.len(), 2, "replay_b len");

    assert_eq!(journal.highest_sequence_nr(&a, 0).await.unwrap(), 3);
    assert_eq!(journal.highest_sequence_nr(&b, 0).await.unwrap(), 2);

    let gap_err = journal.write_messages(vec![repr(&a, 99, None)]).await;
    assert!(
        matches!(gap_err, Err(JournalError::SequenceOutOfOrder { .. })),
        "expected SequenceOutOfOrder, got {gap_err:?}",
    );

    journal.delete_messages_to(&a, 2).await.unwrap();
    let after_delete = journal.replay_messages(&a, 1, u64::MAX, 10).await.unwrap();
    for r in &after_delete {
        assert!(r.sequence_nr > 2, "deleted event leaked: {}", r.sequence_nr);
    }
    assert!(journal.highest_sequence_nr(&a, 0).await.unwrap() >= 3);

    assert_eq!(journal.replay_messages(&b, 1, u64::MAX, 10).await.unwrap().len(), 2);

    let max_replay = journal.replay_messages(&b, 1, u64::MAX, 1).await.unwrap();
    assert_eq!(max_replay.len(), 1, "max argument ignored");
}

/// Extended suite covering edge cases drawn from upstream's
/// `Akka.Persistence.TCK.Journal.JournalSpec`: replay-from-mid,
/// replay-after-delete, idempotent replay, max=0 short-circuit, and
/// concurrent-writer interleaving.
pub async fn journal_extended_suite<J: Journal>(journal: Arc<J>, pid_prefix: &str) {
    let pid = format!("{pid_prefix}-ext");
    journal.write_messages((1..=10u64).map(|i| repr(&pid, i, None)).collect()).await.unwrap();

    // Replay from sequence 4..=7 returns 4 events.
    let mid = journal.replay_messages(&pid, 4, 7, 100).await.unwrap();
    assert_eq!(mid.len(), 4, "replay 4..=7 expected 4 got {}", mid.len());
    assert_eq!(mid.first().unwrap().sequence_nr, 4);
    assert_eq!(mid.last().unwrap().sequence_nr, 7);

    // max=0 must return zero (parity with akka.net's `Replay(max=0)`).
    let none = journal.replay_messages(&pid, 1, 100, 0).await.unwrap();
    assert!(none.is_empty(), "max=0 returned {} entries", none.len());

    // Idempotent replay: calling replay twice yields the same events.
    let r1 = journal.replay_messages(&pid, 1, u64::MAX, 100).await.unwrap();
    let r2 = journal.replay_messages(&pid, 1, u64::MAX, 100).await.unwrap();
    assert_eq!(r1.len(), r2.len(), "non-idempotent replay");
    for (a, b) in r1.iter().zip(r2.iter()) {
        assert_eq!(a.sequence_nr, b.sequence_nr);
        assert_eq!(a.payload, b.payload);
    }

    // Delete up to 5; replay-from-3 must skip the deleted entries.
    journal.delete_messages_to(&pid, 5).await.unwrap();
    let after = journal.replay_messages(&pid, 3, u64::MAX, 100).await.unwrap();
    for r in &after {
        assert!(r.sequence_nr > 5, "deleted event {} leaked", r.sequence_nr);
    }

    // Highest sequence_nr survives the delete (semantic: "we wrote it").
    let high = journal.highest_sequence_nr(&pid, 0).await.unwrap();
    assert!(high >= 10, "highest_sequence_nr regressed: {}", high);
}

/// Concurrent-writer interleaving. Writes batches for two distinct
/// persistence ids concurrently; both replays must observe a contiguous
/// sequence with no cross-id leakage.
pub async fn journal_concurrent_suite<J: Journal>(journal: Arc<J>, pid_prefix: &str) {
    let a = format!("{pid_prefix}-cA");
    let b = format!("{pid_prefix}-cB");
    let ja = journal.clone();
    let jb = journal.clone();
    let a2 = a.clone();
    let b2 = b.clone();
    let h_a = tokio::spawn(async move {
        for i in 1..=20u64 {
            ja.write_messages(vec![repr(&a2, i, None)]).await.unwrap();
        }
    });
    let h_b = tokio::spawn(async move {
        for i in 1..=15u64 {
            jb.write_messages(vec![repr(&b2, i, None)]).await.unwrap();
        }
    });
    h_a.await.unwrap();
    h_b.await.unwrap();
    let ra = journal.replay_messages(&a, 1, u64::MAX, 100).await.unwrap();
    let rb = journal.replay_messages(&b, 1, u64::MAX, 100).await.unwrap();
    assert_eq!(ra.len(), 20);
    assert_eq!(rb.len(), 15);
    for (i, e) in ra.iter().enumerate() {
        assert_eq!(e.sequence_nr, (i + 1) as u64);
        assert_eq!(e.persistence_id, a);
    }
    for (i, e) in rb.iter().enumerate() {
        assert_eq!(e.sequence_nr, (i + 1) as u64);
        assert_eq!(e.persistence_id, b);
    }
}

/// Conformance for optional tag-based querying. Callers gate this on a
/// `supports_tags` capability flag since not every backend implements it.
pub async fn journal_tag_suite<J: Journal>(journal: Arc<J>, pid_prefix: &str) {
    let pid = format!("{pid_prefix}-tag");
    let events = vec![repr(&pid, 1, Some("red")), repr(&pid, 2, Some("blue")), repr(&pid, 3, Some("red"))];
    journal.write_messages(events).await.unwrap();
    let red = journal.events_by_tag("red", 0, 100).await.unwrap();
    assert_eq!(red.len(), 2, "expected 2 red events, got {}", red.len());
    let blue = journal.events_by_tag("blue", 0, 100).await.unwrap();
    assert_eq!(blue.len(), 1, "expected 1 blue event, got {}", blue.len());
}
