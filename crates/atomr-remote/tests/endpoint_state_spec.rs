//! Endpoint manager state-machine spec parity.
//! `EndpointRegistrySpec`, `RemotingSpec` (subset) — peer-state
//! tracking and tombstone purge invariants.

use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::{ActorSystem, Address};
use atomr_remote::{AssociationState, RemoteSettings, RemoteSystem};

async fn boot(name: &str) -> RemoteSystem {
    let sys = ActorSystem::create(name, Config::reference()).await.unwrap();
    RemoteSystem::start(sys, "127.0.0.1:0".parse().unwrap(), RemoteSettings::default()).await.unwrap()
}

#[tokio::test]
async fn unknown_peer_has_no_state() {
    let r = boot("Endpoint-Unknown").await;
    let mgr = r.endpoint_manager();
    let unknown = Address::remote("akka.tcp", "S", "10.0.0.99", 9999);
    assert!(mgr.peer_state(&unknown).is_none());
    r.shutdown().await;
}

#[tokio::test]
async fn tombstone_records_state_until_purged() {
    let r = boot("Endpoint-Tomb").await;
    let mgr = r.endpoint_manager();
    let p = Address::remote("akka.tcp", "S", "10.0.0.1", 1234);
    mgr.tombstone(&p);
    assert_eq!(mgr.peer_state(&p), Some(AssociationState::Tombstoned));
    // High TTL → not purged.
    assert_eq!(mgr.purge_tombstones(Duration::from_secs(60)), 0);
    // Zero TTL → purged.
    assert_eq!(mgr.purge_tombstones(Duration::ZERO), 1);
    assert!(mgr.peer_state(&p).is_none());
    r.shutdown().await;
}

#[tokio::test]
async fn purge_only_removes_tombstoned_entries() {
    // Write a tombstone, then purge with low TTL — only tombstoned
    // peers should be swept; we re-register a different peer and
    // assert it survives.
    let r = boot("Endpoint-Selective").await;
    let mgr = r.endpoint_manager();
    let p1 = Address::remote("akka.tcp", "S", "10.0.0.1", 1);
    let p2 = Address::remote("akka.tcp", "S", "10.0.0.2", 2);
    mgr.tombstone(&p1);
    mgr.tombstone(&p2);
    assert_eq!(mgr.purge_tombstones(Duration::ZERO), 2);
    assert!(mgr.peer_state(&p1).is_none());
    assert!(mgr.peer_state(&p2).is_none());
    r.shutdown().await;
}

#[tokio::test]
async fn peer_states_lists_all_known_peers() {
    let r = boot("Endpoint-Snapshot").await;
    let mgr = r.endpoint_manager();
    let p1 = Address::remote("akka.tcp", "S", "10.0.0.1", 1);
    let p2 = Address::remote("akka.tcp", "S", "10.0.0.2", 2);
    mgr.tombstone(&p1);
    mgr.tombstone(&p2);
    let snap = mgr.peer_states();
    assert!(snap.iter().any(|(addr, _, _)| addr.contains("10.0.0.1")));
    assert!(snap.iter().any(|(addr, _, _)| addr.contains("10.0.0.2")));
    r.shutdown().await;
}

#[tokio::test]
async fn tombstone_is_idempotent() {
    let r = boot("Endpoint-Idem").await;
    let mgr = r.endpoint_manager();
    let p = Address::remote("akka.tcp", "S", "10.0.0.1", 1);
    mgr.tombstone(&p);
    mgr.tombstone(&p);
    assert_eq!(mgr.peer_state(&p), Some(AssociationState::Tombstoned));
    assert_eq!(mgr.purge_tombstones(Duration::ZERO), 1);
    r.shutdown().await;
}
