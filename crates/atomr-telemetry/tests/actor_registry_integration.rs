//! Integration tests exercising the actor-registry probe through a live
//! `ActorSystem`.

use atomr_core::prelude::*;
use atomr_telemetry::TelemetryExtension;

#[derive(Default)]
struct Noop;

#[async_trait]
impl Actor for Noop {
    type Msg = String;
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: String) {}
}

#[tokio::test]
async fn telemetry_records_spawn_and_stop() {
    let sys = ActorSystem::create("T", Config::empty()).await.unwrap();
    let telemetry = TelemetryExtension::new("T", 64).install(&sys);

    let a = sys.actor_of(Props::create(Noop::default), "a").unwrap();
    let _b = sys.actor_of(Props::create(Noop::default), "b").unwrap();

    // Spawn observer is called inline, so snapshot is immediate.
    let snap = telemetry.actors.snapshot();
    assert_eq!(snap.total, 2);
    assert!(snap.flat.iter().any(|a| a.path.ends_with("/user/a")));
    assert!(snap.flat.iter().any(|a| a.path.ends_with("/user/b")));

    a.stop();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let snap2 = telemetry.actors.snapshot();
    assert_eq!(snap2.total, 1, "one actor should have been cleaned up");
    assert_eq!(telemetry.actors.total_spawned(), 2);
    assert_eq!(telemetry.actors.total_stopped(), 1);

    sys.terminate().await;
}

#[tokio::test]
async fn telemetry_accessible_via_from_system() {
    let sys = ActorSystem::create("T2", Config::empty()).await.unwrap();
    TelemetryExtension::new("T2", 32).install(&sys);

    let via = TelemetryExtension::from_system(&sys).expect("handle");
    let _c = sys.actor_of(Props::create(Noop::default), "c").unwrap();
    assert_eq!(via.actors.live_count(), 1);

    sys.terminate().await;
}
