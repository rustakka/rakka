//! ClusterClient / ClusterReceptionist — dispatching to actors addressable by name.
//! akka.net: `Akka.Cluster.Tools/Client/ClusterClient.cs`.
//!
//! Phase 7.D of `docs/full-port-plan.md` — adds initial-contacts +
//! retry/backoff so a non-cluster client can discover the
//! receptionist. The wire transport plugs in once Phase 5/6 ships.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use rakka_core::actor::UntypedActorRef;

#[derive(Default)]
pub struct ClusterReceptionist {
    services: RwLock<HashMap<String, UntypedActorRef>>,
}

impl ClusterReceptionist {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, name: impl Into<String>, r: UntypedActorRef) {
        self.services.write().insert(name.into(), r);
    }

    pub fn lookup(&self, name: &str) -> Option<UntypedActorRef> {
        self.services.read().get(name).cloned()
    }

    pub fn unregister(&self, name: &str) {
        self.services.write().remove(name);
    }

    pub fn registered(&self) -> Vec<String> {
        let mut v: Vec<String> = self.services.read().keys().cloned().collect();
        v.sort();
        v
    }
}

/// Settings for a `ClusterClient`. Mirrors akka.net's
/// `ClusterClientSettings`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClusterClientSettings {
    /// Initial bootstrap addresses to try in order. The first one
    /// that responds wins.
    pub initial_contacts: Vec<String>,
    /// Time to wait for an initial contact before trying the next.
    pub establishing_get_contacts_interval: Duration,
    /// Backoff between full re-attempts after exhausting all initial
    /// contacts.
    pub reconnect_timeout: Duration,
    /// Total attempts before giving up.
    pub max_attempts: u32,
}

impl Default for ClusterClientSettings {
    fn default() -> Self {
        Self {
            initial_contacts: Vec::new(),
            establishing_get_contacts_interval: Duration::from_secs(3),
            reconnect_timeout: Duration::from_secs(10),
            max_attempts: 10,
        }
    }
}

impl ClusterClientSettings {
    pub fn with_initial_contacts<I, S>(mut self, contacts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.initial_contacts = contacts.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }
}

pub struct ClusterClient {
    pub receptionist: Arc<ClusterReceptionist>,
    settings: ClusterClientSettings,
    /// Round-robin cursor over initial contacts.
    contact_cursor: AtomicUsize,
}

impl ClusterClient {
    pub fn new(receptionist: Arc<ClusterReceptionist>) -> Self {
        Self::with_settings(receptionist, ClusterClientSettings::default())
    }

    pub fn with_settings(receptionist: Arc<ClusterReceptionist>, settings: ClusterClientSettings) -> Self {
        Self { receptionist, settings, contact_cursor: AtomicUsize::new(0) }
    }

    /// Direct receptionist lookup (in-process / single-node case).
    pub fn send(&self, name: &str) -> Option<UntypedActorRef> {
        self.receptionist.lookup(name)
    }

    /// Pick the next initial contact (round-robin). Returns `None`
    /// if `initial_contacts` is empty.
    pub fn next_contact(&self) -> Option<String> {
        if self.settings.initial_contacts.is_empty() {
            return None;
        }
        let i = self.contact_cursor.fetch_add(1, Ordering::Relaxed) % self.settings.initial_contacts.len();
        Some(self.settings.initial_contacts[i].clone())
    }

    /// `establish` — drive contact-point discovery using `try_resolve`
    /// until it returns `Some(_)` or `max_attempts` is exhausted.
    /// Sleeps `establishing_get_contacts_interval` between attempts.
    pub async fn establish<F>(&self, mut try_resolve: F) -> Result<UntypedActorRef, ClusterClientError>
    where
        F: FnMut(&str) -> Option<UntypedActorRef>,
    {
        if self.settings.initial_contacts.is_empty() {
            return Err(ClusterClientError::NoContacts);
        }
        for attempt in 0..self.settings.max_attempts {
            let Some(contact) = self.next_contact() else {
                break;
            };
            if let Some(r) = try_resolve(&contact) {
                return Ok(r);
            }
            if attempt + 1 < self.settings.max_attempts {
                tokio::time::sleep(self.settings.establishing_get_contacts_interval).await;
            }
        }
        Err(ClusterClientError::Exhausted { attempts: self.settings.max_attempts })
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClusterClientError {
    #[error("no initial contacts configured")]
    NoContacts,
    #[error("contact-point resolution failed after {attempts} attempts")]
    Exhausted { attempts: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_core::actor::Inbox;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn receptionist_register_lookup() {
        let rec = ClusterReceptionist::new();
        let inbox = Inbox::<u32>::new("svc");
        rec.register("svc", inbox.actor_ref().as_untyped());
        let c = ClusterClient::new(rec);
        assert!(c.send("svc").is_some());
    }

    #[test]
    fn next_contact_round_robins() {
        let rec = ClusterReceptionist::new();
        let s = ClusterClientSettings::default().with_initial_contacts(vec!["a", "b", "c"]);
        let c = ClusterClient::with_settings(rec, s);
        assert_eq!(c.next_contact().as_deref(), Some("a"));
        assert_eq!(c.next_contact().as_deref(), Some("b"));
        assert_eq!(c.next_contact().as_deref(), Some("c"));
        assert_eq!(c.next_contact().as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn establish_returns_first_resolved_contact() {
        let rec = ClusterReceptionist::new();
        let inbox = Inbox::<u32>::new("svc");
        let target = inbox.actor_ref().as_untyped();
        let target_clone = target.clone();
        let s = ClusterClientSettings::default().with_initial_contacts(vec!["a", "b"]).with_max_attempts(3);
        let c = ClusterClient::with_settings(rec, s);
        let calls = AtomicU32::new(0);
        let result = c
            .establish(|contact| {
                calls.fetch_add(1, Ordering::SeqCst);
                if contact == "b" {
                    Some(target_clone.clone())
                } else {
                    None
                }
            })
            .await
            .unwrap();
        assert_eq!(result.path(), target.path());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn establish_no_contacts_errors() {
        let rec = ClusterReceptionist::new();
        let c = ClusterClient::new(rec);
        let r = c.establish(|_| None).await;
        assert!(matches!(r, Err(ClusterClientError::NoContacts)));
    }

    #[tokio::test]
    async fn establish_exhausts_after_max_attempts() {
        let rec = ClusterReceptionist::new();
        let s = ClusterClientSettings {
            initial_contacts: vec!["x".into()],
            establishing_get_contacts_interval: Duration::from_millis(1),
            reconnect_timeout: Duration::from_millis(1),
            max_attempts: 3,
        };
        let c = ClusterClient::with_settings(rec, s);
        let r = c.establish(|_| None).await;
        assert!(matches!(r, Err(ClusterClientError::Exhausted { attempts: 3 })));
    }

    #[test]
    fn registered_lists_services_sorted() {
        let rec = ClusterReceptionist::new();
        let inbox = Inbox::<u32>::new("x");
        rec.register("zebra", inbox.actor_ref().as_untyped());
        rec.register("alpha", inbox.actor_ref().as_untyped());
        rec.register("middle", inbox.actor_ref().as_untyped());
        assert_eq!(rec.registered(), vec!["alpha", "middle", "zebra"]);
    }
}
