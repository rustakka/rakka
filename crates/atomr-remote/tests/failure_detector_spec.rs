//! Failure detector spec parity. akka.net:
//! `Remote.Tests/AccrualFailureDetectorSpec`,
//! `Remote.Tests/PhiAccrualModelBasedSpecs`,
//! `Remote.Tests/DeadlineFailureDetectorSpec`,
//! `Remote.Tests/FailureDetectorRegistrySpec`.
//!
//! These tests cover the public observable contract of the three detector
//! types and the per-address registry. They use short, real-time pauses
//! (well under 50ms) tuned against detector parameters so they remain
//! deterministic without sleeping for substantial wall-clock time.

use std::sync::Arc;
use std::time::Duration;

use atomr_core::actor::Address;
use atomr_remote::failure_detector::FailureDetector;
use atomr_remote::{
    DeadlineFailureDetector, FailureDetectorRegistry, PhiAccrualFailureDetector,
};

/// Build a phi-accrual detector tuned for fast tests: tiny first-heartbeat
/// estimate and min std deviation so a few ms past the acceptable pause is
/// enough to drive phi well above threshold.
fn fast_phi(threshold: f64, acceptable_pause_ms: u64) -> PhiAccrualFailureDetector {
    PhiAccrualFailureDetector::new(
        threshold,
        200,
        Duration::from_millis(1),
        Duration::from_millis(acceptable_pause_ms),
        Duration::from_millis(1),
    )
}

// ---- PhiAccrualFailureDetector ------------------------------------------

/// akka.net `AccrualFailureDetectorSpec`:
/// "must be available after a series of successful heartbeats". A brand-new
/// detector that has never received a heartbeat is reported as available
/// (akka.net default — it must not flag peers it has not yet observed).
#[test]
fn phi_new_detector_with_no_heartbeats_is_available() {
    let d = fast_phi(8.0, 5);
    assert!(d.is_available());
    assert!(!d.is_monitoring());
    assert!(d.since_last_heartbeat().is_none());
    assert_eq!(d.phi(), 0.0);
}

/// After a heartbeat the detector is monitoring and remains available
/// while subsequent checks happen well within the acceptable pause.
#[test]
fn phi_remains_available_within_expected_interval() {
    let d = fast_phi(8.0, 50);
    d.heartbeat();
    assert!(d.is_monitoring());
    assert!(d.is_available());
    // A second heartbeat at a normal cadence keeps it available.
    std::thread::sleep(Duration::from_millis(2));
    d.heartbeat();
    assert!(d.is_available());
    assert!(d.since_last_heartbeat().is_some());
}

/// Long pause past the acceptable pause drives phi above threshold and the
/// detector reports unavailable.
#[test]
fn phi_unavailable_after_long_pause() {
    // 1ms acceptable pause, 1ms first-heartbeat estimate, 1ms min std dev:
    // ~20ms past the heartbeat is many standard deviations into the tail.
    let d = fast_phi(8.0, 1);
    d.heartbeat();
    assert!(d.is_available());
    std::thread::sleep(Duration::from_millis(25));
    assert!(!d.is_available(), "phi={} should exceed threshold", d.phi());
    assert!(d.phi() >= 8.0);
}

/// `reset` clears history and last-heartbeat, returning the detector to
/// the "no heartbeat yet" state (akka.net `FailureDetector.Reset`).
#[test]
fn phi_reset_returns_to_initial_state() {
    let d = fast_phi(8.0, 1);
    d.heartbeat();
    std::thread::sleep(Duration::from_millis(15));
    assert!(!d.is_available());
    d.reset();
    assert!(d.is_available());
    assert!(!d.is_monitoring());
    assert!(d.since_last_heartbeat().is_none());
}

// ---- DeadlineFailureDetector --------------------------------------------

/// akka.net `DeadlineFailureDetectorSpec`: a detector with no heartbeats
/// is available (cannot fail something it has not started watching).
#[test]
fn deadline_new_detector_is_available() {
    let d = DeadlineFailureDetector::new(Duration::from_millis(20));
    assert!(d.is_available());
    assert!(!d.is_monitoring());
}

/// The deadline detector is available strictly until its deadline elapses.
#[test]
fn deadline_available_until_deadline_then_unavailable() {
    let d = DeadlineFailureDetector::new(Duration::from_millis(20));
    d.heartbeat();
    assert!(d.is_monitoring());
    assert!(d.is_available(), "available immediately after heartbeat");
    // Still within the deadline.
    std::thread::sleep(Duration::from_millis(5));
    assert!(d.is_available(), "still available within deadline");
    // After the deadline elapses.
    std::thread::sleep(Duration::from_millis(30));
    assert!(!d.is_available(), "unavailable once deadline has elapsed");
}

/// A heartbeat after the deadline elapsed restores availability.
#[test]
fn deadline_heartbeat_restores_availability() {
    let d = DeadlineFailureDetector::new(Duration::from_millis(15));
    d.heartbeat();
    std::thread::sleep(Duration::from_millis(25));
    assert!(!d.is_available());
    d.heartbeat();
    assert!(d.is_available());
}

// ---- FailureDetectorRegistry --------------------------------------------

/// akka.net `FailureDetectorRegistrySpec`: an unknown address is reported
/// as available because the registry has not yet started monitoring it.
#[test]
fn registry_unknown_address_is_available() {
    let reg = FailureDetectorRegistry::default_phi();
    let a = Address::remote("akka.tcp", "S", "10.0.0.1", 1111);
    assert!(reg.is_available(&a));
    assert!(reg.addresses().is_empty());
}

/// The registry tracks a separate detector per address — failure on one
/// address does not affect another. Uses a fast factory so the test stays
/// deterministic.
#[test]
fn registry_isolates_detectors_per_address() {
    let factory: Arc<dyn Fn() -> Arc<dyn FailureDetector> + Send + Sync> =
        Arc::new(|| Arc::new(fast_phi(8.0, 1)));
    let reg = FailureDetectorRegistry::new(factory);

    let a = Address::remote("akka.tcp", "S", "10.0.0.1", 1111);
    let b = Address::remote("akka.tcp", "S", "10.0.0.2", 2222);

    reg.heartbeat(&a);
    reg.heartbeat(&b);
    assert!(reg.is_available(&a));
    assert!(reg.is_available(&b));

    // Let `a` go silent while `b` keeps heart-beating.
    std::thread::sleep(Duration::from_millis(25));
    reg.heartbeat(&b);
    assert!(reg.is_available(&b), "b's fresh heartbeat keeps it alive");
    assert!(!reg.is_available(&a), "a's missed heartbeats marked it down");

    let addrs = reg.addresses();
    assert_eq!(addrs.len(), 2);
}

/// Removing an address drops its detector. A subsequent `is_available`
/// query falls back to the "unknown == available" rule, and a fresh
/// heartbeat starts a new detector for the address.
#[test]
fn registry_remove_clears_detector() {
    let reg = FailureDetectorRegistry::default_phi();
    let a = Address::remote("akka.tcp", "S", "10.0.0.5", 5555);

    reg.heartbeat(&a);
    assert_eq!(reg.addresses().len(), 1);
    reg.remove(&a);
    assert!(reg.addresses().is_empty());
    assert!(reg.is_available(&a), "removed address is treated as unknown");

    // Re-registering after remove starts a brand-new detector.
    reg.heartbeat(&a);
    assert!(reg.is_available(&a));
}
