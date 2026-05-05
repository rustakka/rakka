//! Substream + recovery operator spec parity.
//! `FlowGroupBySpec`, `FlowSplitWhenSpec`, `FlowSplitAfterSpec`,
//! `FlowPrefixAndTailSpec`, `FlowRecoverWithSpec`, `FlowRecoverWithRetriesSpec`.
//!
//! These tests assert the public substream / recovery surface of
//! `atomr_streams` matches the corresponding upstream JVM/.NET specs:
//!
//! - `group_by`: keys preserve per-key ordering; cap drops new keys; the
//!   upstream finishing terminates every open sub-source.
//! - `split_when` puts the pivot in the **new** chunk; `split_after`
//!   keeps the pivot in the **previous** chunk.
//! - `prefix_and_tail` with `n == 0` yields an empty prefix and the
//!   full tail; with `n > len` yields the full prefix and an empty tail.
//! - `recover` with `Some(_)` injects a fallback element and terminates;
//!   with `None` drops the error silently and terminates.
//! - `recover_with` swaps the failing tail for a replacement source.
//! - `recover_with_retries(n, factory)` exhausts attempts and stops on
//!   the next error vs continues replaying while attempts remain.
//! - `map_error` / `select_error` rewrite the error type without
//!   collapsing successes.

use std::collections::HashMap;

use atomr_streams::{
    group_by, map_error, prefix_and_tail, recover, recover_with, recover_with_retries, select_error,
    split_after, split_when, Sink, Source,
};

// ---------- group_by ----------

#[tokio::test]
async fn group_by_preserves_per_key_ordering() {
    // Items routed to the same key must appear on the per-key
    // sub-source in their original upstream order. :
    // `FlowGroupBySpec.GroupBy_must_work_with_normal_user_scenario`.
    let s = Source::from_iter(vec![10, 21, 30, 41, 50, 61]);
    let outer = group_by(s, 2, |x: &i32| *x % 2);
    let pairs = Sink::collect(outer).await;
    let mut by_key: HashMap<i32, Vec<i32>> = HashMap::new();
    for (k, sub) in pairs {
        by_key.insert(k, Sink::collect(sub).await);
    }
    assert_eq!(by_key.get(&0), Some(&vec![10, 30, 50]));
    assert_eq!(by_key.get(&1), Some(&vec![21, 41, 61]));
}

#[tokio::test]
async fn group_by_drops_keys_past_max_substreams_cap() {
    // Cap at 2 keys; only the first two distinct keys should produce
    // sub-sources. : `FlowGroupBySpec.GroupBy_must_fail_when_value
    // _of_too_many_substreams` (we silently drop instead of failing —
    // the rust port elects the "drop new keys" policy).
    let s = Source::from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let outer = group_by(s, 2, |x: &i32| *x % 3);
    let pairs = Sink::collect(outer).await;
    assert_eq!(pairs.len(), 2);
    let mut by_key: HashMap<i32, Vec<i32>> = HashMap::new();
    for (k, sub) in pairs {
        by_key.insert(k, Sink::collect(sub).await);
    }
    // Keys 1 and 2 open first; key 0 is past the cap and is dropped.
    assert!(by_key.contains_key(&1));
    assert!(by_key.contains_key(&2));
    assert!(!by_key.contains_key(&0));
    assert_eq!(by_key.get(&1), Some(&vec![1, 4, 7]));
    assert_eq!(by_key.get(&2), Some(&vec![2, 5, 8]));
}

#[tokio::test]
async fn group_by_finishes_every_substream_when_upstream_finishes() {
    // Sub-sources must terminate cleanly once the parent source
    // exhausts. : `FlowGroupBySpec.GroupBy_must_work_with_empty
    // _input_stream` / `should_close_substreams_after_main_stream
    // _completion`.
    let s = Source::from_iter(vec![1, 2, 3, 4]);
    let outer = group_by(s, 4, |x: &i32| *x % 2);
    let pairs = Sink::collect(outer).await;
    assert_eq!(pairs.len(), 2);
    for (_k, sub) in pairs {
        // Each sub-source must terminate (collect completes) — without
        // upstream-finish-propagation this would hang.
        let v = Sink::collect(sub).await;
        assert!(!v.is_empty());
    }
}

#[tokio::test]
async fn group_by_handles_empty_upstream() {
    // No keys, no sub-sources.
    let s: Source<i32> = Source::empty();
    let outer = group_by(s, 4, |x: &i32| *x);
    let pairs = Sink::collect(outer).await;
    assert!(pairs.is_empty());
}

// ---------- split_when ----------

#[tokio::test]
async fn split_when_places_pivot_in_new_chunk() {
    // Predicate-true element belongs to the **new** substream (it
    // marks the boundary).
    let s = Source::from_iter(vec![1, 2, 10, 3, 4, 20, 5]);
    let outer = split_when(s, |x: &i32| *x >= 10);
    let mut chunks = Vec::new();
    for sub in Sink::collect(outer).await {
        chunks.push(Sink::collect(sub).await);
    }
    assert_eq!(chunks, vec![vec![1, 2], vec![10, 3, 4], vec![20, 5]]);
}

#[tokio::test]
async fn split_when_with_no_match_yields_single_chunk() {
    let s = Source::from_iter(vec![1, 2, 3]);
    let outer = split_when(s, |x: &i32| *x >= 99);
    let mut chunks = Vec::new();
    for sub in Sink::collect(outer).await {
        chunks.push(Sink::collect(sub).await);
    }
    assert_eq!(chunks, vec![vec![1, 2, 3]]);
}

// ---------- split_after ----------

#[tokio::test]
async fn split_after_keeps_pivot_in_previous_chunk() {
    // Predicate-true element ends the current substream and stays
    // with it.
    let s = Source::from_iter(vec![1, 2, 10, 3, 4, 20, 5]);
    let outer = split_after(s, |x: &i32| *x >= 10);
    let mut chunks = Vec::new();
    for sub in Sink::collect(outer).await {
        chunks.push(Sink::collect(sub).await);
    }
    assert_eq!(chunks, vec![vec![1, 2, 10], vec![3, 4, 20], vec![5]]);
}

#[tokio::test]
async fn split_after_pivot_at_end_does_not_emit_empty_chunk() {
    // When the splitting element is the very last upstream element
    // there should be no trailing empty substream emitted.
    let s = Source::from_iter(vec![1, 2, 3]);
    let outer = split_after(s, |x: &i32| *x == 3);
    let mut chunks = Vec::new();
    for sub in Sink::collect(outer).await {
        chunks.push(Sink::collect(sub).await);
    }
    assert_eq!(chunks, vec![vec![1, 2, 3]]);
}

// ---------- prefix_and_tail ----------

#[tokio::test]
async fn prefix_and_tail_with_zero_yields_empty_prefix_and_full_tail() {
    // n == 0: prefix is empty, tail receives every element.
    let s = Source::from_iter(vec![1, 2, 3, 4]);
    let outer = prefix_and_tail(s, 0);
    let mut pairs = Sink::collect(outer).await;
    assert_eq!(pairs.len(), 1);
    let (prefix, tail) = pairs.pop().unwrap();
    assert!(prefix.is_empty());
    let rest = Sink::collect(tail).await;
    assert_eq!(rest, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn prefix_and_tail_with_n_greater_than_len_yields_full_prefix_and_empty_tail() {
    // n > len: prefix gets everything, tail is empty.
    // `FlowPrefixAndTailSpec.must_complete_with_short_prefix
    // _and_empty_tail_when_input_is_short`.
    let s = Source::from_iter(vec![1, 2, 3]);
    let outer = prefix_and_tail(s, 10);
    let mut pairs = Sink::collect(outer).await;
    assert_eq!(pairs.len(), 1);
    let (prefix, tail) = pairs.pop().unwrap();
    assert_eq!(prefix, vec![1, 2, 3]);
    let rest = Sink::collect(tail).await;
    assert!(rest.is_empty());
}

#[tokio::test]
async fn prefix_and_tail_with_exact_n_yields_full_prefix_and_empty_tail() {
    let s = Source::from_iter(vec![7, 8, 9]);
    let outer = prefix_and_tail(s, 3);
    let mut pairs = Sink::collect(outer).await;
    let (prefix, tail) = pairs.pop().unwrap();
    assert_eq!(prefix, vec![7, 8, 9]);
    assert!(Sink::collect(tail).await.is_empty());
}

// ---------- recover ----------

#[tokio::test]
async fn recover_emits_fallback_element_and_terminates_on_first_error() {
    // `Some(fallback)` → emit it, then terminate. Subsequent upstream
    // elements after the error are dropped. :
    // `FlowRecoverSpec.A_Recover_must_recover_when_there_is_a_failure`.
    let s: Source<Result<i32, &'static str>> =
        Source::from_iter(vec![Ok(1), Ok(2), Err("boom"), Ok(99), Ok(100)]);
    let recovered = recover(s, |_e| Some(-1));
    let collected = Sink::collect(recovered).await;
    assert_eq!(collected, vec![1, 2, -1]);
}

#[tokio::test]
async fn recover_with_none_drops_the_error_and_terminates() {
    // `None` → swallow the error, terminate without emitting
    // anything more.`None` shape.
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Ok(2), Err("e"), Ok(3)]);
    let recovered = recover(s, |_e| None);
    let collected = Sink::collect(recovered).await;
    assert_eq!(collected, vec![1, 2]);
}

// ---------- recover_with ----------

#[tokio::test]
async fn recover_with_switches_stream_tail_on_error() {
    // First Err triggers replacement source; pre-error Oks flow
    // through. : `FlowRecoverWithSpec.A_RecoverWith_must
    // _recover_when_there_is_a_failure`.
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Ok(2), Err("e"), Ok(99)]);
    let replacement: Source<i32> = Source::from_iter(vec![100, 200, 300]);
    let recovered = recover_with(s, replacement);
    let collected = Sink::collect(recovered).await;
    assert_eq!(collected, vec![1, 2, 100, 200, 300]);
}

#[tokio::test]
async fn recover_with_passes_through_when_upstream_succeeds() {
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Ok(2), Ok(3)]);
    let replacement: Source<i32> = Source::from_iter(vec![100]);
    let recovered = recover_with(s, replacement);
    let collected = Sink::collect(recovered).await;
    assert_eq!(collected, vec![1, 2, 3]);
}

// ---------- recover_with_retries ----------

#[tokio::test]
async fn recover_with_retries_exhausts_attempts_then_stops() {
    // max_attempts = 2: first two errors trigger replacements; the
    // third error trips the stream. :
    // `FlowRecoverWithRetriesSpec.must_terminate_with_failure_after_max
    // _retries`.
    let s: Source<Result<i32, &'static str>> =
        Source::from_iter(vec![Ok(1), Err("e1"), Err("e2"), Err("e3"), Ok(999)]);
    let mut counter = 0;
    let recovered = recover_with_retries(s, 2, move || {
        counter += 1;
        Source::from_iter(vec![counter * 10])
    });
    let collected = Sink::collect(recovered).await;
    // 1, then e1 → 10, then e2 → 20, then e3 with no attempts left
    // trips the stream so the trailing Ok(999) never appears.
    assert_eq!(collected, vec![1, 10, 20]);
}

#[tokio::test]
async fn recover_with_retries_continues_while_attempts_remain() {
    // Fewer errors than attempts: every error replaces, the upstream
    // tail still flows.
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Err("e1"), Ok(2)]);
    let recovered = recover_with_retries(s, 5, || Source::from_iter(vec![100, 200]));
    let collected = Sink::collect(recovered).await;
    // 1 → e1 → replacement (100, 200) → upstream Ok(2) flows because
    // attempts are not exhausted.
    assert_eq!(collected, vec![1, 100, 200, 2]);
}

#[tokio::test]
async fn recover_with_retries_zero_attempts_stops_on_first_error() {
    // max_attempts = 0: any error trips immediately, no replacement
    // is materialized.
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Err("e"), Ok(2)]);
    let recovered = recover_with_retries(s, 0, || Source::from_iter(vec![777]));
    let collected = Sink::collect(recovered).await;
    assert_eq!(collected, vec![1]);
}

// ---------- map_error / select_error ----------

#[tokio::test]
async fn map_error_rewrites_error_payload_without_collapsing_successes() {
    // Successes flow through Ok-shaped; errors get their payload
    // remapped.
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Err("x"), Ok(2)]);
    let mapped = map_error(s, |e| format!("wrapped:{e}"));
    let collected = Sink::collect(mapped).await;
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], Ok(1));
    assert_eq!(collected[1], Err("wrapped:x".to_string()));
    assert_eq!(collected[2], Ok(2));
}

#[tokio::test]
async fn select_error_alias_matches_map_error_behavior() {
    let s: Source<Result<i32, &'static str>> = Source::from_iter(vec![Ok(1), Err("boom")]);
    let mapped = select_error(s, |e| e.to_uppercase());
    let collected = Sink::collect(mapped).await;
    assert_eq!(collected, vec![Ok(1), Err("BOOM".to_string())]);
}
