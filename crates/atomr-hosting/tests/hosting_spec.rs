//! Hosting builder spec parity. Asserts
//! the construct-and-shutdown contract, DI service resolution from
//! the hosted system, and on-start hook firing order.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use atomr_config::Config;
use atomr_hosting::ActorSystemBuilder;

struct Greeter(&'static str);

#[tokio::test]
async fn builder_creates_system_with_default_config() {
    let hosted = ActorSystemBuilder::new("hb-default").build().await.unwrap();
    assert_eq!(hosted.system.name(), "hb-default");
    hosted.terminate().await;
}

#[tokio::test]
async fn with_config_threads_through_to_system() {
    let cfg = Config::reference();
    let hosted = ActorSystemBuilder::new("hb-config").with_config(cfg).build().await.unwrap();
    // The reference config provides actor.provider; just confirm the
    // system booted with it.
    assert_eq!(hosted.system.name(), "hb-config");
    hosted.terminate().await;
}

#[tokio::test]
async fn with_services_registers_into_di_container() {
    let hosted = ActorSystemBuilder::new("hb-di")
        .with_services(|c| {
            c.register::<Greeter, _>(|| Arc::new(Greeter("hi")));
        })
        .build()
        .await
        .unwrap();
    let g = hosted.container.resolve::<Greeter>().unwrap();
    assert_eq!(g.0, "hi");
    hosted.terminate().await;
}

#[tokio::test]
async fn on_start_hooks_fire_in_registration_order() {
    let log: Arc<std::sync::Mutex<Vec<u32>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let l1 = log.clone();
    let l2 = log.clone();
    let l3 = log.clone();
    let hosted = ActorSystemBuilder::new("hb-hooks")
        .on_start(move |_| l1.lock().unwrap().push(1))
        .on_start(move |_| l2.lock().unwrap().push(2))
        .on_start(move |_| l3.lock().unwrap().push(3))
        .build()
        .await
        .unwrap();
    let order = log.lock().unwrap().clone();
    assert_eq!(order, vec![1, 2, 3]);
    hosted.terminate().await;
}

#[tokio::test]
async fn on_start_hook_sees_running_system() {
    let saw_name: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let s = saw_name.clone();
    let hosted = ActorSystemBuilder::new("hb-running")
        .on_start(move |sys| *s.lock().unwrap() = Some(sys.name().to_string()))
        .build()
        .await
        .unwrap();
    assert_eq!(saw_name.lock().unwrap().clone().as_deref(), Some("hb-running"));
    hosted.terminate().await;
}

#[tokio::test]
async fn services_and_hooks_compose() {
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let hosted = ActorSystemBuilder::new("hb-compose")
        .with_services(|sc| {
            sc.register::<Greeter, _>(|| Arc::new(Greeter("compose")));
        })
        .on_start(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
        })
        .build()
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let g = hosted.container.resolve::<Greeter>().unwrap();
    assert_eq!(g.0, "compose");
    hosted.terminate().await;
}
