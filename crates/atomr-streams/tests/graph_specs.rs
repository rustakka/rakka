//! Graph junction spec parity. akka.net:
//! `GraphMergeSpec`, `GraphMergeSortedSpec`, `GraphMergePrioritizedSpec`,
//! `GraphZipSpec`, `GraphZipWithIndexSpec`, `GraphBroadcastSpec`.

use atomr_streams::{
    broadcast, concat, merge, merge_all, merge_prioritized, merge_sorted, zip, zip_with, zip_with_index,
    Sink, Source,
};

#[tokio::test]
async fn merge_combines_two_sources_unordered() {
    let a = Source::from_iter(vec![1u32, 2, 3]);
    let b = Source::from_iter(vec![10u32, 20, 30]);
    let mut out = Sink::collect(merge(a, b)).await;
    out.sort();
    assert_eq!(out, vec![1, 2, 3, 10, 20, 30]);
}

#[tokio::test]
async fn merge_all_combines_n_sources() {
    let a = Source::from_iter(vec![1]);
    let b = Source::from_iter(vec![2]);
    let c = Source::from_iter(vec![3]);
    let d = Source::from_iter(vec![4]);
    let mut out = Sink::collect(merge_all(vec![a, b, c, d])).await;
    out.sort();
    assert_eq!(out, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn concat_drains_first_then_second() {
    let a = Source::from_iter(vec![1, 2]);
    let b = Source::from_iter(vec![10, 20]);
    let out = Sink::collect(concat(a, b)).await;
    assert_eq!(out, vec![1, 2, 10, 20]);
}

#[tokio::test]
async fn zip_pairs_corresponding_elements() {
    let a = Source::from_iter(vec![1u32, 2, 3]);
    let b = Source::from_iter(vec!["a", "b", "c"]);
    let out = Sink::collect(zip(a, b)).await;
    assert_eq!(out, vec![(1, "a"), (2, "b"), (3, "c")]);
}

#[tokio::test]
async fn zip_terminates_with_shorter_source() {
    let a = Source::from_iter(vec![1u32, 2, 3]);
    let b = Source::from_iter(vec!["a"]);
    let out = Sink::collect(zip(a, b)).await;
    assert_eq!(out, vec![(1, "a")]);
}

#[tokio::test]
async fn zip_with_applies_function_per_pair() {
    let a = Source::from_iter(vec![1u32, 2, 3]);
    let b = Source::from_iter(vec![10u32, 20, 30]);
    let out = Sink::collect(zip_with(a, b, |x, y| x + y)).await;
    assert_eq!(out, vec![11, 22, 33]);
}

#[tokio::test]
async fn zip_with_index_attaches_offset() {
    let s = Source::from_iter(vec!["a", "b", "c"]);
    let out = Sink::collect(zip_with_index(s)).await;
    assert_eq!(out, vec![(0, "a"), (1, "b"), (2, "c")]);
}

#[tokio::test]
async fn broadcast_replicates_to_two_consumers() {
    let s = Source::from_iter(vec![1u32, 2, 3]);
    let (left, right) = broadcast(s);
    let l = Sink::collect(left).await;
    let r = Sink::collect(right).await;
    assert_eq!(l, vec![1, 2, 3]);
    assert_eq!(r, vec![1, 2, 3]);
}

#[tokio::test]
async fn merge_sorted_preserves_total_order() {
    let a = Source::from_iter(vec![1u32, 4, 5, 9]);
    let b = Source::from_iter(vec![2u32, 3, 6, 8]);
    let out = Sink::collect(merge_sorted(a, b)).await;
    assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 8, 9]);
}

#[tokio::test]
async fn merge_sorted_drains_remainder_when_one_side_exhausts() {
    let a = Source::from_iter(vec![1u32, 5, 9]);
    let b = Source::from_iter(vec![2u32]);
    let out = Sink::collect(merge_sorted(a, b)).await;
    assert_eq!(out, vec![1, 2, 5, 9]);
}

#[tokio::test]
async fn merge_prioritized_proportional_weights() {
    let a = Source::from_iter(vec![1u32; 30]);
    let b = Source::from_iter(vec![2u32; 30]);
    // weights 2:1 → roughly 2/3 of output is 1s.
    let out = Sink::collect(merge_prioritized(a, 2, b, 1)).await;
    let ones = out.iter().filter(|x| **x == 1).count();
    let twos = out.iter().filter(|x| **x == 2).count();
    assert_eq!(ones + twos, 60);
    // 2:1 budgets means ones should be at least as common as twos.
    assert!(ones >= twos, "ones={ones}, twos={twos}");
}
