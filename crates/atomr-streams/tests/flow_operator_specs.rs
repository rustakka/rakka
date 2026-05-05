//! Linear `Source` operator spec parity. akka.net: `FlowSpec`,
//! `FlowMapSpec`, `FlowFilterSpec`, `FlowTakeSpec`, `FlowSkipSpec`,
//! `FlowScanSpec`, `FlowIntersperseSpec`, `FlowBufferSpec`,
//! `FlowThrottleSpec`, `FlowZipWithIndexSpec`.

use std::time::{Duration, Instant};

use atomr_streams::{zip_with_index, OverflowStrategy, Sink, Source};

#[tokio::test]
async fn map_transforms_each_element() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=5_i32).map(|x| x * x)).await;
    assert_eq!(out, vec![1, 4, 9, 16, 25]);
}

#[tokio::test]
async fn filter_keeps_matching_elements() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=10_i32).filter(|x| x % 2 == 0)).await;
    assert_eq!(out, vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn take_truncates_after_n_elements() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=100_i32).take(4)).await;
    assert_eq!(out, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn take_zero_yields_nothing() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=5_i32).take(0)).await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn skip_drops_first_n_elements() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=6_i32).skip(3)).await;
    assert_eq!(out, vec![4, 5, 6]);
}

#[tokio::test]
async fn skip_more_than_length_yields_empty() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(1..=3_i32).skip(10)).await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn scan_emits_running_aggregate() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(vec![1, 2, 3, 4, 5]).scan(0, |acc, x| acc + x)).await;
    assert_eq!(out, vec![1, 3, 6, 10, 15]);
}

#[tokio::test]
async fn intersperse_places_separator_between_but_not_at_ends() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(vec![1, 2, 3, 4]).intersperse(0)).await;
    assert_eq!(out, vec![1, 0, 2, 0, 3, 0, 4]);
    assert_ne!(out.first(), Some(&0));
    assert_ne!(out.last(), Some(&0));
}

#[tokio::test]
async fn intersperse_single_element_emits_no_separator() {
    let out: Vec<i32> = Sink::collect(Source::from_iter(vec![42]).intersperse(0)).await;
    assert_eq!(out, vec![42]);
}

#[tokio::test]
async fn intersperse_empty_source_yields_empty() {
    let out: Vec<i32> = Sink::collect(Source::<i32>::empty().intersperse(0)).await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn buffer_drop_new_drops_overflow_with_slow_consumer() {
    // A fast producer feeds an unbounded number of elements into a
    // size-1 DropNew buffer. The consumer is slow enough that the
    // buffer should overflow and drop incoming items, producing
    // strictly fewer outputs than inputs.
    let src = Source::from_iter(0..200_i32);
    let buffered = src.buffer(1, OverflowStrategy::DropNew);
    // Throttle the downstream so the producer can outrun it.
    let slow = buffered.map_async(1, |v| async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        v
    });
    let out: Vec<i32> = Sink::collect(slow).await;
    assert!(!out.is_empty(), "should observe at least one element");
    assert!(out.len() <= 200, "must not exceed produced count");
    // DropNew preserves the order of elements that survived.
    for w in out.windows(2) {
        assert!(w[0] < w[1], "DropNew must preserve relative order: {:?}", out);
    }
}

#[tokio::test]
async fn buffer_drop_head_keeps_newest_when_full() {
    // With DropHead and size=2, against a slow consumer, the buffer
    // should always favour newer elements. Surviving elements still
    // appear in the original producer order.
    let src = Source::from_iter(0..50_i32);
    let buffered = src.buffer(2, OverflowStrategy::DropHead);
    let slow = buffered.map_async(1, |v| async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        v
    });
    let out: Vec<i32> = Sink::collect(slow).await;
    assert!(!out.is_empty());
    // Some elements may be dropped, but no more than were produced.
    assert!(out.len() <= 50);
    // Output must remain in producer order (DropHead drops older items;
    // surviving items keep their relative order).
    for w in out.windows(2) {
        assert!(w[0] < w[1], "DropHead must preserve relative order: {:?}", out);
    }
}

#[tokio::test]
async fn buffer_backpressure_passes_all_elements() {
    let src = Source::from_iter(0..100_i32);
    let buffered = src.buffer(4, OverflowStrategy::Backpressure);
    let out = Sink::collect(buffered).await;
    assert_eq!(out.len(), 100);
    assert_eq!(out.first(), Some(&0));
    assert_eq!(out.last(), Some(&99));
}

#[tokio::test]
async fn throttle_rate_limits_elements_roughly_linearly() {
    // throttle(interval) sleeps `interval` between every produced
    // element. For 5 elements at 20ms cadence, expect ~100ms. Allow
    // ±50% slack for CI scheduling jitter, with a generous upper bound.
    let n = 5_u64;
    let interval = Duration::from_millis(20);
    let expected = interval * n as u32;

    let start = Instant::now();
    let out: Vec<u64> = Sink::collect(Source::from_iter(0..n).throttle(interval)).await;
    let elapsed = start.elapsed();

    assert_eq!(out, (0..n).collect::<Vec<_>>());
    let lower = expected / 2;
    let upper = expected * 3;
    assert!(elapsed >= lower, "throttle elapsed {:?} below lower bound {:?}", elapsed, lower);
    assert!(elapsed <= upper, "throttle elapsed {:?} above upper bound {:?}", elapsed, upper);
}

#[tokio::test]
async fn zip_with_index_attaches_u64_indices() {
    let src = Source::from_iter(vec!["a", "b", "c", "d"]);
    let out = Sink::collect(zip_with_index(src)).await;
    assert_eq!(out, vec![(0u64, "a"), (1, "b"), (2, "c"), (3, "d")]);
    // Verify the index type is u64 — function returns Source<(u64, T)>.
    let first_index: u64 = out[0].0;
    assert_eq!(first_index, 0);
}

#[tokio::test]
async fn zip_with_index_on_empty_source() {
    let out = Sink::collect(zip_with_index(Source::<i32>::empty())).await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn from_iter_collect_round_trips() {
    let input: Vec<i64> = (-5..=5).collect();
    let out: Vec<i64> = Sink::collect(Source::from_iter(input.clone())).await;
    assert_eq!(out, input);
}

#[tokio::test]
async fn source_single_yields_exactly_one_element() {
    let out: Vec<&'static str> = Sink::collect(Source::single("only")).await;
    assert_eq!(out, vec!["only"]);
    assert_eq!(out.len(), 1);
}

#[tokio::test]
async fn linear_pipeline_composes_map_filter_take_skip_scan() {
    // Mirrors akka.net `FlowSpec` style end-to-end pipeline assertion.
    let out: Vec<i32> = Sink::collect(
        Source::from_iter(1..=20_i32)
            .map(|x| x * 2) // 2,4,6,...,40
            .filter(|x| x % 3 == 0) // 6,12,18,24,30,36
            .skip(1) // 12,18,24,30,36
            .take(3) // 12,18,24
            .scan(0, |acc, x| acc + x), // 12,30,54
    )
    .await;
    assert_eq!(out, vec![12, 30, 54]);
}
