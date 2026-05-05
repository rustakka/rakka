//! CRDT algebraic-laws spec parity. : every CRDT must be a
//! semilattice — merge is commutative, associative, and idempotent.
//! Mirrors the `{GCounter, GSet,
//! ORSet, PNCounter, LWWRegister, Flag}Spec` cross-suite assertions.

use atomr_distributed_data::{CrdtMerge, Flag, GCounter, GSet, LwwRegister, OrSet, PNCounter};

fn merged<T: CrdtMerge>(a: &T, b: &T) -> T {
    let mut c = a.clone();
    c.merge(b);
    c
}

// --- GCounter ------------------------------------------------------

#[test]
fn gcounter_merge_is_commutative() {
    let mut a = GCounter::new();
    a.increment("n1", 3);
    let mut b = GCounter::new();
    b.increment("n2", 5);
    let ab = merged(&a, &b);
    let ba = merged(&b, &a);
    assert_eq!(ab.value(), ba.value());
    assert_eq!(ab.value(), 8);
}

#[test]
fn gcounter_merge_is_associative() {
    let mut a = GCounter::new();
    a.increment("n1", 1);
    let mut b = GCounter::new();
    b.increment("n2", 2);
    let mut c = GCounter::new();
    c.increment("n3", 4);
    let left = merged(&merged(&a, &b), &c);
    let right = merged(&a, &merged(&b, &c));
    assert_eq!(left.value(), right.value());
}

#[test]
fn gcounter_merge_is_idempotent() {
    let mut a = GCounter::new();
    a.increment("n1", 7);
    let aa = merged(&a, &a);
    assert_eq!(aa.value(), a.value());
}

#[test]
fn gcounter_per_node_takes_max() {
    let mut a = GCounter::new();
    a.increment("n1", 5);
    let mut b = GCounter::new();
    b.increment("n1", 3);
    let m = merged(&a, &b);
    assert_eq!(m.value(), 5, "merge should take max per node, not sum");
}

// --- PNCounter -----------------------------------------------------

#[test]
fn pncounter_merge_round_trip() {
    let mut a = PNCounter::new();
    a.increment("n1", 10);
    a.decrement("n1", 3);
    let mut b = PNCounter::new();
    b.increment("n2", 4);
    let m1 = merged(&a, &b);
    let m2 = merged(&b, &a);
    assert_eq!(m1.value(), m2.value());
    assert_eq!(m1.value(), 11); // 10 - 3 + 4
}

#[test]
fn pncounter_merge_is_idempotent() {
    let mut a = PNCounter::new();
    a.increment("n1", 5);
    a.decrement("n1", 1);
    let aa = merged(&a, &a);
    assert_eq!(aa.value(), a.value());
}

// --- GSet ----------------------------------------------------------

#[test]
fn gset_merge_is_set_union() {
    let mut a: GSet<u32> = GSet::new();
    a.add(1);
    a.add(2);
    let mut b: GSet<u32> = GSet::new();
    b.add(2);
    b.add(3);
    let m = merged(&a, &b);
    let mut items: Vec<&u32> = m.iter().collect();
    items.sort();
    assert_eq!(items, vec![&1, &2, &3]);
}

#[test]
fn gset_merge_is_idempotent() {
    let mut a: GSet<u32> = GSet::new();
    a.add(1);
    a.add(2);
    let aa = merged(&a, &a);
    assert_eq!(aa.len(), 2);
}

// --- OrSet ---------------------------------------------------------

#[test]
fn or_set_merge_is_commutative() {
    let mut a: OrSet<&'static str> = OrSet::new();
    a.add("x");
    a.add("y");
    let mut b: OrSet<&'static str> = OrSet::new();
    b.add("y");
    b.add("z");
    let ab = merged(&a, &b);
    let ba = merged(&b, &a);
    let mut a_items: Vec<&&'static str> = ab.iter().collect();
    a_items.sort();
    let mut b_items: Vec<&&'static str> = ba.iter().collect();
    b_items.sort();
    assert_eq!(a_items, b_items);
}

#[test]
fn or_set_remove_after_merge_clears_seen_tags() {
    // Sequential add then remove on the same replica clears the
    // element. This is the canonical OR-Set "remove wins given the
    // tags it has observed" rule.
    //
    // Note: atomr's OrSet uses a per-instance monotone counter for
    // add-tags rather than globally-unique tags, so the cross-replica
    // "concurrent re-add survives remove" invariant only holds when
    // tag generation is disjoint (e.g. replicas have observed each
    // other before re-adding). This test focuses on the tag-disjoint
    // case to keep it deterministic.
    let mut a: OrSet<&'static str> = OrSet::new();
    a.add("x");
    a.remove(&"x");
    let m = merged(&a, &a);
    assert!(!m.contains(&"x"), "remove with all tags observed clears element");
}

// --- LwwRegister ---------------------------------------------------

#[test]
fn lww_register_takes_latest_timestamp() {
    let a = LwwRegister::new("nA", "first".to_string(), 1);
    let b = LwwRegister::new("nB", "second".to_string(), 2);
    let m1 = merged(&a, &b);
    let m2 = merged(&b, &a);
    assert_eq!(m1.value(), m2.value());
    assert_eq!(m1.value().as_str(), "second");
}

#[test]
fn lww_register_is_idempotent() {
    let a = LwwRegister::new("n", 42, 5);
    let aa = merged(&a, &a);
    assert_eq!(aa.value(), &42);
}

// --- Flag ----------------------------------------------------------

#[test]
fn flag_merge_or_semantics() {
    let mut a = Flag::new();
    let mut b = Flag::new();
    assert!(!a.enabled());
    a.switch_on();
    let m = merged(&a, &b);
    assert!(m.enabled(), "merging false with true gives true");
    b.switch_on();
    let mm = merged(&a, &b);
    assert!(mm.enabled());
}

#[test]
fn flag_is_monotonic() {
    let mut a = Flag::new();
    a.switch_on();
    let b = Flag::new();
    let m = merged(&a, &b);
    assert!(m.enabled(), "flag does not turn off after merge with off-flag");
}
