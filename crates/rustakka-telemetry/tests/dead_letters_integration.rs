//! Integration: verify failed `ActorRef::tell` after `stop()` produces a
//! dead letter in the telemetry feed.

use rustakka_core::prelude::*;
use rustakka_telemetry::TelemetryExtension;

#[derive(Default)]
struct Noop;

#[async_trait]
impl Actor for Noop {
    type Msg = String;
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: String) {}
}

#[tokio::test]
async fn failed_tell_records_dead_letter() {
    let sys = ActorSystem::create("DL", Config::empty()).await.unwrap();
    let telemetry = TelemetryExtension::new("DL", 32).install(&sys);

    let target = sys.actor_of(Props::create(Noop::default), "t").unwrap();

    // Trigger stop + allow the runtime to drop the mailbox receiver.
    target.stop();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    target.tell("nobody home".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(telemetry.dead_letters.total_count() >= 1);
    let recent = telemetry.dead_letters.recent(10);
    assert!(recent.iter().any(|r| r.recipient.ends_with("/user/t")));

    sys.terminate().await;
}
