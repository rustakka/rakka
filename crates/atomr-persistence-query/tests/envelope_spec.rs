//! Spec for persistence-query envelope/offset invariants.
//!
//! Covers:
//! - `EventEnvelope::from(PersistentRepr)` field carry-over and the
//!   in-memory backend's `offset == sequence_nr` semantics.
//! - `Offset::as_sequence` mapping for each variant.
//! - `Offset::default() == NoOffset`.
//! - `EventEnvelope` is `Send + 'static`-compatible (compile-time).
//!
//! Bincode round-trip for `Offset` is intentionally skipped: at the
//! time of writing, `Offset` does not derive `serde::Serialize` /
//! `Deserialize`, so the assertion is not applicable.

use atomr_persistence::PersistentRepr;
use atomr_persistence_query::{EventEnvelope, Offset};

fn sample_repr() -> PersistentRepr {
    PersistentRepr {
        persistence_id: "pid-42".into(),
        sequence_nr: 7,
        payload: vec![0xDE, 0xAD, 0xBE, 0xEF],
        manifest: "evt".into(),
        writer_uuid: "writer-1".into(),
        deleted: false,
        tags: vec!["red".into(), "hot".into()],
    }
}

#[test]
fn envelope_from_persistent_repr_carries_all_fields() {
    let repr = sample_repr();
    let env: EventEnvelope = repr.clone().into();

    assert_eq!(env.persistence_id, repr.persistence_id);
    assert_eq!(env.sequence_nr, repr.sequence_nr);
    assert_eq!(env.payload, repr.payload);
    assert_eq!(env.tags, repr.tags);
}

#[test]
fn envelope_offset_matches_sequence_nr_for_in_memory_backend() {
    let repr = sample_repr();
    let seq = repr.sequence_nr;
    let env: EventEnvelope = repr.into();
    assert_eq!(env.offset, seq, "in-memory backend uses sequence_nr as offset");
}

#[test]
fn no_offset_maps_to_zero_sequence() {
    assert_eq!(Offset::NoOffset.as_sequence(), Some(0));
}

#[test]
fn sequence_offset_round_trips_value() {
    assert_eq!(Offset::Sequence(0).as_sequence(), Some(0));
    assert_eq!(Offset::Sequence(1).as_sequence(), Some(1));
    assert_eq!(Offset::Sequence(u64::MAX).as_sequence(), Some(u64::MAX));
}

#[test]
fn time_based_offset_has_no_sequence() {
    assert_eq!(Offset::TimeBased(0).as_sequence(), None);
    assert_eq!(Offset::TimeBased(u128::MAX).as_sequence(), None);
}

#[test]
fn offset_default_is_no_offset() {
    assert_eq!(Offset::default(), Offset::NoOffset);
}

#[test]
fn event_envelope_is_send_and_static() {
    fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<EventEnvelope>();
}
