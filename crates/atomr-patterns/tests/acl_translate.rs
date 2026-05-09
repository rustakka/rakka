//! Anti-Corruption Layer: 100 external items in, only those the
//! translator returns `Some` for come out internal.

use std::time::Duration;

use atomr_config::Config;
use atomr_core::actor::ActorSystem;
use atomr_patterns::acl::{AntiCorruption, Translator};
use atomr_patterns::topology::Topology;

struct EvenOnly;
impl Translator for EvenOnly {
    type External = i64;
    type Internal = i64;
    fn translate(&self, ext: i64) -> Option<i64> {
        if ext % 2 == 0 {
            Some(ext * 10)
        } else {
            None
        }
    }
}

#[tokio::test]
async fn even_inputs_pass_through_translated_odd_inputs_dropped() {
    let system = ActorSystem::create("acl", Config::reference()).await.unwrap();
    let topology = AntiCorruption::<i64, i64>::builder(EvenOnly).build();
    let mut handles = topology.materialize(&system).await.unwrap();

    for n in 0..100i64 {
        handles.input.send(n).unwrap();
    }
    drop(handles.input); // close so the task drains and exits

    let mut got = Vec::new();
    while let Ok(Some(v)) = tokio::time::timeout(Duration::from_secs(1), handles.output.recv()).await {
        got.push(v);
    }

    let expected: Vec<i64> = (0..100i64).filter(|n| n % 2 == 0).map(|n| n * 10).collect();
    assert_eq!(got, expected);

    system.terminate().await;
}
