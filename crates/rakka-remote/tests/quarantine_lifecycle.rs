//! Phase 5 — quarantine lifecycle queries on `EndpointManager`.
//!
//! Verifies that `tombstone(target)` followed by `purge_tombstones`
//! actually drops the entry, and that `peer_state(target)` reports
//! the right state at each transition.

use std::time::Duration;

use rakka_config::Config;
use rakka_core::actor::{ActorSystem, Address};
use rakka_remote::{AssociationState, RemoteSettings, RemoteSystem};

async fn boot(name: &str) -> RemoteSystem {
    let sys = ActorSystem::create(name, Config::reference()).await.unwrap();
    RemoteSystem::start(sys, "127.0.0.1:0".parse().unwrap(), RemoteSettings::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn tombstone_then_purge() {
    let r = boot("QLifecycle").await;
    let mgr = r.endpoint_manager().clone();
    // No association attempted → state is None.
    let phantom = Address::remote("akka.tcp", "Sys", "127.0.0.1", 1);
    assert!(mgr.peer_state(&phantom).is_none());

    mgr.tombstone(&phantom);
    assert_eq!(mgr.peer_state(&phantom), Some(AssociationState::Tombstoned));

    // Purge with a long TTL → no removal yet.
    assert_eq!(mgr.purge_tombstones(Duration::from_secs(60)), 0);
    assert_eq!(mgr.peer_state(&phantom), Some(AssociationState::Tombstoned));

    // Purge with a 0-duration TTL → entry gone.
    let purged = mgr.purge_tombstones(Duration::from_millis(0));
    assert_eq!(purged, 1);
    assert!(mgr.peer_state(&phantom).is_none());

    r.shutdown().await;
}
