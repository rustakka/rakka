//! Reachability spec parity. akka.net: `ReachabilitySpec`.
//!
//! A subject is "reachable" iff *no* observer reports it unreachable.
//! Observers can recover (a previously-reported-unreachable subject
//! becomes reachable again once the observer reports `reachable`),
//! and can mark a subject `terminated` (final).

use atomr_cluster::{Reachability, ReachabilityStatus};
use atomr_core::actor::Address;

fn addr(host: &str) -> Address {
    Address { protocol: "atomr".into(), system: "S".into(), host: Some(host.into()), port: Some(0) }
}

#[test]
fn brand_new_subject_is_reachable_by_default() {
    let r = Reachability::new();
    assert!(r.is_reachable(&addr("a")));
}

#[test]
fn one_observer_reporting_unreachable_makes_subject_unreachable() {
    let mut r = Reachability::new();
    r.unreachable(addr("o1"), addr("subj"));
    assert!(!r.is_reachable(&addr("subj")));
}

#[test]
fn observer_recovery_returns_subject_to_reachable_when_no_other_observer_objects() {
    let mut r = Reachability::new();
    r.unreachable(addr("o1"), addr("subj"));
    assert!(!r.is_reachable(&addr("subj")));
    r.reachable(addr("o1"), addr("subj"));
    assert!(r.is_reachable(&addr("subj")));
}

#[test]
fn subject_remains_unreachable_while_any_observer_objects() {
    let mut r = Reachability::new();
    r.unreachable(addr("o1"), addr("subj"));
    r.unreachable(addr("o2"), addr("subj"));
    r.reachable(addr("o1"), addr("subj"));
    // o2 still says unreachable.
    assert!(!r.is_reachable(&addr("subj")));
}

#[test]
fn terminated_makes_subject_unreachable_permanently() {
    let mut r = Reachability::new();
    r.terminated(addr("o1"), addr("subj"));
    assert_eq!(r.status(&addr("subj")), ReachabilityStatus::Terminated);
    // Even an observer reporting reachable does not undo Terminated.
    r.reachable(addr("o1"), addr("subj"));
    assert_eq!(r.status(&addr("subj")), ReachabilityStatus::Terminated);
}
