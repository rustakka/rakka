//! Lease spec parity. akka.net: `Akka.Coordination.Lease` invariants.
//! Mirrors the assertions from `LeaseSpec` (KubernetesLease tests fold
//! into the same trait-level invariants in akka.net).

use atomr_coordination::{InMemoryLease, Lease, LeaseError, LeaseRegistry};

#[tokio::test]
async fn brand_new_lease_is_unheld() {
    let l = InMemoryLease::new();
    assert!(l.check_lease().await.is_none());
}

#[tokio::test]
async fn acquire_marks_owner() {
    let l = InMemoryLease::new();
    assert!(l.acquire("worker-1").await.unwrap());
    assert_eq!(l.check_lease().await.as_deref(), Some("worker-1"));
}

#[tokio::test]
async fn re_acquire_by_same_owner_is_idempotent() {
    let l = InMemoryLease::new();
    assert!(l.acquire("me").await.unwrap());
    assert!(l.acquire("me").await.unwrap(), "same owner re-acquire should succeed");
    assert_eq!(l.check_lease().await.as_deref(), Some("me"));
}

#[tokio::test]
async fn second_owner_gets_already_held_error() {
    let l = InMemoryLease::new();
    l.acquire("a").await.unwrap();
    let err = l.acquire("b").await.unwrap_err();
    match err {
        LeaseError::AlreadyHeld(by) => assert_eq!(by, "a"),
        other => panic!("expected AlreadyHeld, got {other:?}"),
    }
}

#[tokio::test]
async fn release_clears_owner() {
    let l = InMemoryLease::new();
    l.acquire("me").await.unwrap();
    l.release("me").await.unwrap();
    assert!(l.check_lease().await.is_none());
}

#[tokio::test]
async fn release_by_non_owner_errors() {
    let l = InMemoryLease::new();
    l.acquire("me").await.unwrap();
    let err = l.release("not-me").await.unwrap_err();
    assert!(matches!(err, LeaseError::NotHeld));
    // The original owner still holds it.
    assert_eq!(l.check_lease().await.as_deref(), Some("me"));
}

#[tokio::test]
async fn release_when_unheld_errors() {
    let l = InMemoryLease::new();
    let err = l.release("anyone").await.unwrap_err();
    assert!(matches!(err, LeaseError::NotHeld));
}

#[tokio::test]
async fn after_release_a_new_owner_can_acquire() {
    let l = InMemoryLease::new();
    l.acquire("a").await.unwrap();
    l.release("a").await.unwrap();
    assert!(l.acquire("b").await.unwrap());
    assert_eq!(l.check_lease().await.as_deref(), Some("b"));
}

#[tokio::test]
async fn registry_returns_the_same_lease_for_same_name() {
    let r = LeaseRegistry::new();
    let a = r.get_or_create("svc");
    let b = r.get_or_create("svc");
    a.acquire("first").await.unwrap();
    assert_eq!(b.check_lease().await.as_deref(), Some("first"));
}

#[tokio::test]
async fn registry_distinguishes_lease_names() {
    let r = LeaseRegistry::new();
    let a = r.get_or_create("svc-1");
    let b = r.get_or_create("svc-2");
    a.acquire("X").await.unwrap();
    assert_eq!(b.check_lease().await, None);
    b.acquire("Y").await.unwrap();
    assert_eq!(a.check_lease().await.as_deref(), Some("X"));
    assert_eq!(b.check_lease().await.as_deref(), Some("Y"));
}
