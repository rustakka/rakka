//! Cluster-client + receptionist spec parity.
//! `ClusterClientSettingsSpec`, `ClusterClientSpec`, and the
//! receptionist registration assertions inside both.

use std::time::Duration;

use atomr_cluster_tools::{ClusterClient, ClusterClientSettings, ClusterReceptionist};
use atomr_config::Config;
use atomr_core::actor::{Actor, ActorSystem, Context, Props};

#[derive(Default)]
struct Sink;

#[async_trait::async_trait]
impl Actor for Sink {
    type Msg = ();
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
}

#[tokio::test]
async fn receptionist_register_then_lookup_returns_actor() {
    let sys = ActorSystem::create("rcp-1", Config::reference()).await.unwrap();
    let r = sys.actor_of(Props::create(Sink::default), "svc").unwrap().as_untyped();
    let recpt = ClusterReceptionist::new();
    recpt.register("svc", r.clone());
    let found = recpt.lookup("svc").unwrap();
    assert_eq!(found.path(), r.path());
    sys.terminate().await;
}

#[tokio::test]
async fn unregister_removes_entry() {
    let sys = ActorSystem::create("rcp-2", Config::reference()).await.unwrap();
    let r = sys.actor_of(Props::create(Sink::default), "svc").unwrap().as_untyped();
    let recpt = ClusterReceptionist::new();
    recpt.register("svc", r);
    recpt.unregister("svc");
    assert!(recpt.lookup("svc").is_none());
    sys.terminate().await;
}

#[tokio::test]
async fn registered_returns_sorted_names() {
    let sys = ActorSystem::create("rcp-3", Config::reference()).await.unwrap();
    let r = sys.actor_of(Props::create(Sink::default), "svc").unwrap().as_untyped();
    let recpt = ClusterReceptionist::new();
    recpt.register("zeta", r.clone());
    recpt.register("alpha", r.clone());
    recpt.register("mu", r);
    assert_eq!(recpt.registered(), vec!["alpha".to_string(), "mu".to_string(), "zeta".to_string()]);
    sys.terminate().await;
}

#[tokio::test]
async fn lookup_unknown_name_returns_none() {
    let recpt = ClusterReceptionist::new();
    assert!(recpt.lookup("never").is_none());
}

#[test]
fn settings_default_has_empty_contacts() {
    let s = ClusterClientSettings::default();
    assert!(s.initial_contacts.is_empty());
    assert!(s.max_attempts >= 1);
}

#[test]
fn settings_with_initial_contacts_overrides() {
    let s = ClusterClientSettings::default().with_initial_contacts(["a", "b", "c"]);
    assert_eq!(s.initial_contacts, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
}

#[test]
fn settings_with_max_attempts_overrides() {
    let s = ClusterClientSettings::default().with_max_attempts(42);
    assert_eq!(s.max_attempts, 42);
}

#[test]
fn settings_default_intervals_are_nonzero() {
    let s = ClusterClientSettings::default();
    assert!(s.establishing_get_contacts_interval > Duration::ZERO);
    assert!(s.reconnect_timeout > Duration::ZERO);
}

#[tokio::test]
async fn client_send_routes_to_registered_actor() {
    let sys = ActorSystem::create("client-send", Config::reference()).await.unwrap();
    let r = sys.actor_of(Props::create(Sink::default), "svc").unwrap().as_untyped();
    let recpt = ClusterReceptionist::new();
    recpt.register("svc", r.clone());
    let client = ClusterClient::new(recpt);
    let found = client.send("svc").unwrap();
    assert_eq!(found.path(), r.path());
    assert!(client.send("missing").is_none());
    sys.terminate().await;
}

#[test]
fn next_contact_round_robins() {
    let recpt = ClusterReceptionist::new();
    let client = ClusterClient::with_settings(
        recpt,
        ClusterClientSettings::default().with_initial_contacts(["a", "b", "c"]),
    );
    let v = (0..6).filter_map(|_| client.next_contact()).collect::<Vec<_>>();
    assert_eq!(v, vec!["a", "b", "c", "a", "b", "c"]);
}

#[test]
fn next_contact_with_no_initial_contacts_is_none() {
    let recpt = ClusterReceptionist::new();
    let client = ClusterClient::new(recpt);
    assert!(client.next_contact().is_none());
}
