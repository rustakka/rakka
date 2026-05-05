//! Member ordering parity.,
//! `MemberOrderingModelBasedTests`.
//!
//! The age ordering is "oldest first": a lower `up_number` sorts
//! before a higher one, with address as the deterministic tie-break.
//! Cluster singletons use this to elect the oldest member as host.

use atomr_cluster::{Member, MemberStatus};
use atomr_core::actor::Address;

fn addr(host: &str, port: u16) -> Address {
    Address { protocol: "atomr".into(), system: "S".into(), host: Some(host.into()), port: Some(port) }
}

fn member(host: &str, port: u16, up_number: i32) -> Member {
    let mut m = Member::new(addr(host, port), Vec::new());
    m.up_number = up_number;
    m.status = MemberStatus::Up;
    m
}

#[test]
fn lower_up_number_sorts_first() {
    let young = member("a", 1, 5);
    let old = member("b", 2, 1);
    let mut v = vec![young.clone(), old.clone()];
    Member::sort_by_age(&mut v);
    assert_eq!(v[0], old);
    assert_eq!(v[1], young);
}

#[test]
fn ties_break_on_address() {
    let a = member("a", 1, 0);
    let b = member("b", 1, 0);
    let c = member("a", 2, 0);
    let mut v = vec![b.clone(), c.clone(), a.clone()];
    Member::sort_by_age(&mut v);
    assert_eq!(v, vec![a, c, b]);
}

#[test]
fn oldest_returns_lowest_up_number() {
    let v = vec![member("a", 1, 7), member("b", 2, 3), member("c", 3, 5)];
    assert_eq!(Member::oldest(&v).unwrap().up_number, 3);
}

#[test]
fn oldest_of_empty_is_none() {
    let v: Vec<Member> = Vec::new();
    assert!(Member::oldest(&v).is_none());
}

#[test]
fn ordering_is_transitive() {
    let a = member("a", 1, 1);
    let b = member("b", 1, 2);
    let c = member("c", 1, 3);
    assert_eq!(Member::age_ordering(&a, &b), std::cmp::Ordering::Less);
    assert_eq!(Member::age_ordering(&b, &c), std::cmp::Ordering::Less);
    assert_eq!(Member::age_ordering(&a, &c), std::cmp::Ordering::Less);
}
