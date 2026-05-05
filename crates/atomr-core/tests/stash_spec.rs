//! BoundedStash spec parity. akka.net: `StashMailboxSpec`,
//! `Stash` overflow tests.

use atomr_core::actor::{BoundedStash, StashOverflow, StashResult};

#[test]
fn empty_stash_drains_to_empty_vec() {
    let mut s = BoundedStash::<u32>::new(4, StashOverflow::Reject);
    assert!(s.is_empty());
    let drained = s.unstash_all();
    assert!(drained.is_empty());
}

#[test]
fn unstash_all_preserves_fifo_order() {
    let mut s = BoundedStash::<u32>::new(8, StashOverflow::Reject);
    for i in 0..5u32 {
        s.stash(i);
    }
    let out = s.unstash_all();
    assert_eq!(out, vec![0, 1, 2, 3, 4]);
    assert!(s.is_empty(), "drained stash is empty");
}

#[test]
fn pop_returns_oldest_first() {
    let mut s = BoundedStash::<u32>::new(8, StashOverflow::Reject);
    s.stash(10);
    s.stash(20);
    assert_eq!(s.pop(), Some(10));
    assert_eq!(s.pop(), Some(20));
    assert_eq!(s.pop(), None);
}

#[test]
fn reject_preserves_buffer_contents() {
    let mut s = BoundedStash::<u32>::new(2, StashOverflow::Reject);
    s.stash(1);
    s.stash(2);
    let r = s.stash(3);
    assert_eq!(r, StashResult::Rejected(3));
    let out = s.unstash_all();
    assert_eq!(out, vec![1, 2]);
}

#[test]
fn drop_oldest_pushes_back_and_returns_dropped() {
    let mut s = BoundedStash::<u32>::new(3, StashOverflow::DropOldest);
    for i in 1u32..=3 {
        s.stash(i);
    }
    let r = s.stash(4);
    assert_eq!(r, StashResult::DroppedOldest(1));
    assert_eq!(s.unstash_all(), vec![2, 3, 4]);
}

#[test]
fn drop_newest_returns_dropped_marker() {
    let mut s = BoundedStash::<u32>::new(2, StashOverflow::DropNewest);
    s.stash(1);
    s.stash(2);
    let r = s.stash(3);
    assert_eq!(r, StashResult::DroppedNewest);
    assert_eq!(s.unstash_all(), vec![1, 2]);
}

#[test]
fn capacity_one_drop_oldest_works() {
    let mut s = BoundedStash::<u32>::new(1, StashOverflow::DropOldest);
    s.stash(1);
    let r = s.stash(2);
    assert_eq!(r, StashResult::DroppedOldest(1));
    assert_eq!(s.unstash_all(), vec![2]);
}

#[test]
#[should_panic]
fn zero_capacity_panics() {
    let _: BoundedStash<u32> = BoundedStash::new(0, StashOverflow::Reject);
}

#[test]
fn len_and_is_full_track_state() {
    let mut s = BoundedStash::<u32>::new(2, StashOverflow::Reject);
    assert_eq!(s.len(), 0);
    assert!(!s.is_full());
    s.stash(1);
    assert_eq!(s.len(), 1);
    assert!(!s.is_full());
    s.stash(2);
    assert!(s.is_full());
}
