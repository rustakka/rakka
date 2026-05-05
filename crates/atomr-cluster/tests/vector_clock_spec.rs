//! Vector-clock spec parity.
//!
//! Asserts the documented happens-before relation across the four
//! cases (Same / Before / After / Concurrent) and that `merge` is the
//! pointwise max (idempotent, commutative, associative).

use atomr_cluster::{VectorClock, VectorRelation};

#[test]
fn empty_clocks_are_same() {
    let a = VectorClock::new();
    let b = VectorClock::new();
    assert_eq!(a.compare(&b), VectorRelation::Same);
}

#[test]
fn tick_makes_caller_strictly_after() {
    let mut a = VectorClock::new();
    let b = VectorClock::new();
    a.tick("N1");
    assert_eq!(a.compare(&b), VectorRelation::After);
    assert_eq!(b.compare(&a), VectorRelation::Before);
}

#[test]
fn disjoint_ticks_are_concurrent() {
    let mut a = VectorClock::new();
    let mut b = VectorClock::new();
    a.tick("N1");
    b.tick("N2");
    assert_eq!(a.compare(&b), VectorRelation::Concurrent);
    assert_eq!(b.compare(&a), VectorRelation::Concurrent);
}

#[test]
fn merge_is_pointwise_max() {
    let mut a = VectorClock::new();
    let mut b = VectorClock::new();
    a.tick("X");
    a.tick("X");
    a.tick("Y");
    b.tick("X");
    b.tick("Y");
    b.tick("Y");
    let m = a.merge(&b);
    assert_eq!(m.versions["X"], 2);
    assert_eq!(m.versions["Y"], 2);
}

#[test]
fn merge_is_commutative() {
    let mut a = VectorClock::new();
    let mut b = VectorClock::new();
    a.tick("X");
    a.tick("Y");
    b.tick("Y");
    b.tick("Z");
    assert_eq!(a.merge(&b), b.merge(&a));
}

#[test]
fn merge_is_idempotent() {
    let mut a = VectorClock::new();
    a.tick("X");
    a.tick("Y");
    assert_eq!(a.merge(&a), a);
}

#[test]
fn merge_is_associative() {
    let mut a = VectorClock::new();
    let mut b = VectorClock::new();
    let mut c = VectorClock::new();
    a.tick("X");
    b.tick("Y");
    c.tick("Z");
    let left = a.merge(&b).merge(&c);
    let right = a.merge(&b.merge(&c));
    assert_eq!(left, right);
}

#[test]
fn after_merge_each_is_le_merge() {
    let mut a = VectorClock::new();
    let mut b = VectorClock::new();
    a.tick("X");
    a.tick("X");
    b.tick("Y");
    let m = a.merge(&b);
    assert!(matches!(a.compare(&m), VectorRelation::Before | VectorRelation::Same));
    assert!(matches!(b.compare(&m), VectorRelation::Before | VectorRelation::Same));
}
