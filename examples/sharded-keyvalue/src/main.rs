//! Phase 14.D — runnable demo of cluster-sharding.
//!
//! Spins up a single-node `ShardRegion` that hosts an in-memory key/value
//! store. Each entity is keyed by a string id; the message extractor
//! buckets entities into shards by their first character. Demonstrates:
//!
//! * `MessageExtractor` for entity/shard routing
//! * `ShardCoordinator` allocation
//! * `ShardRegion::deliver` end-to-end
//! * `PassivationTracker` reporting idle entities

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use rakka_cluster_sharding::{MessageExtractor, PassivationTracker, ShardCoordinator, ShardRegion};

/// One key/value command. The first character of the key picks the shard.
#[derive(Clone, Debug)]
enum KvCmd {
    Put { key: String, value: String },
    Get { key: String },
}

struct KvExtractor;

impl MessageExtractor for KvExtractor {
    type Message = KvCmd;
    fn entity_id(&self, m: &Self::Message) -> String {
        match m {
            KvCmd::Put { key, .. } | KvCmd::Get { key } => key.clone(),
        }
    }
    fn shard_id(&self, m: &Self::Message) -> String {
        let key = match m {
            KvCmd::Put { key, .. } | KvCmd::Get { key } => key,
        };
        let first = key.chars().next().unwrap_or('_');
        format!("shard-{}", first.to_ascii_lowercase())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let coord = Arc::new(ShardCoordinator::new());
    let store: Arc<Mutex<std::collections::HashMap<String, String>>> =
        Arc::new(Mutex::new(Default::default()));
    let passivation = Arc::new(PassivationTracker::new());
    let store_for_factory = store.clone();
    let pass_for_factory = passivation.clone();
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_factory = counter.clone();

    let region = ShardRegion::new(
        "node-1",
        Arc::new(KvExtractor),
        coord,
        Arc::new(move || {
            let store = store_for_factory.clone();
            let pass = pass_for_factory.clone();
            let counter = counter_for_factory.clone();
            Box::new(move |id: &str, msg: KvCmd| {
                pass.record_activity(id);
                counter.fetch_add(1, Ordering::Relaxed);
                match msg {
                    KvCmd::Put { key, value } => {
                        store.lock().unwrap().insert(key.clone(), value.clone());
                        println!("[entity {id}] PUT {key} = {value}");
                    }
                    KvCmd::Get { key } => {
                        let v = store.lock().unwrap().get(&key).cloned();
                        println!("[entity {id}] GET {key} -> {v:?}");
                    }
                }
            })
        }),
    );

    // Drive a workload across two shards.
    region.deliver(KvCmd::Put { key: "alpha".into(), value: "v1".into() });
    region.deliver(KvCmd::Put { key: "bravo".into(), value: "v2".into() });
    region.deliver(KvCmd::Put { key: "alpha".into(), value: "v3".into() });
    region.deliver(KvCmd::Get { key: "alpha".into() });
    region.deliver(KvCmd::Get { key: "bravo".into() });

    println!("--");
    println!("active shards: {}", region.shard_count());
    println!("delivered messages: {}", counter.load(Ordering::Relaxed));

    // Show passivation candidates after a short idle window.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let now = Instant::now();
    let idle = passivation.idle_since(Duration::from_millis(20));
    println!("idle entities (>20ms): {idle:?}");
    let _ = now;
    Ok(())
}
