//! Journal conformance suite. Every provider crate runs this against its
//! backend to validate semantic parity with the in-memory reference impl.

use std::sync::Arc;

use rustakka_persistence::{Journal, JournalError, PersistentRepr};

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

/// Conformance for optional tag-based querying. Callers gate this on a
/// `supports_tags` capability flag since not every backend implements it.
pub async fn journal_tag_suite<J: Journal>(journal: Arc<J>, pid_prefix: &str) {
    let pid = format!("{pid_prefix}-tag");
    let events = vec![
        repr(&pid, 1, Some("red")),
        repr(&pid, 2, Some("blue")),
        repr(&pid, 3, Some("red")),
    ];
    journal.write_messages(events).await.unwrap();
    let red = journal.events_by_tag("red", 0, 100).await.unwrap();
    assert_eq!(red.len(), 2, "expected 2 red events, got {}", red.len());
    let blue = journal.events_by_tag("blue", 0, 100).await.unwrap();
    assert_eq!(blue.len(), 1, "expected 1 blue event, got {}", blue.len());
}
