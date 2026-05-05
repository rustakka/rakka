//! ActorPath / Address spec parity. akka.net:
//! `ActorPathSpec`, `AddressSpec`, `RelativeActorPathSpec`,
//! `RemotePathParsingSpec`.

use atomr_core::actor::{ActorPath, Address};

#[test]
fn local_address_round_trips() {
    let a = Address::local("MySys");
    assert!(a.has_local_scope());
    assert!(!a.has_global_scope());
    assert_eq!(a.system, "MySys");
    assert_eq!(a.host, None);
    assert_eq!(a.port, None);
}

#[test]
fn remote_address_carries_host_and_port() {
    let a = Address::remote("akka.tcp", "Sys", "10.0.0.1", 2552);
    assert!(!a.has_local_scope());
    assert!(a.has_global_scope());
    assert_eq!(a.host.as_deref(), Some("10.0.0.1"));
    assert_eq!(a.port, Some(2552));
}

#[test]
fn parse_local_address() {
    let a = Address::parse("akka://MySys").unwrap();
    assert_eq!(a.system, "MySys");
    assert_eq!(a.host, None);
}

#[test]
fn parse_remote_address() {
    let a = Address::parse("akka.tcp://MySys@10.0.0.1:2552").unwrap();
    assert_eq!(a.system, "MySys");
    assert_eq!(a.host.as_deref(), Some("10.0.0.1"));
    assert_eq!(a.port, Some(2552));
    assert_eq!(a.protocol, "akka.tcp");
}

#[test]
fn parse_invalid_address_yields_none() {
    assert!(Address::parse("not a url").is_none());
    assert!(Address::parse("").is_none());
}

#[test]
fn root_path_displays_with_address_prefix() {
    let p = ActorPath::root(Address::local("S"));
    assert_eq!(p.to_string(), "akka://S/");
    assert_eq!(p.depth(), 0);
}

#[test]
fn child_paths_compose() {
    let p = ActorPath::root(Address::local("S")).child("user").child("foo").child("bar");
    assert_eq!(p.name(), "bar");
    assert_eq!(p.depth(), 3);
    assert_eq!(p.to_string(), "akka://S/user/foo/bar");
}

#[test]
fn parent_strips_one_element() {
    let p = ActorPath::root(Address::local("S")).child("a").child("b").child("c");
    let parent = p.parent().unwrap();
    assert_eq!(parent.name(), "b");
    assert_eq!(parent.depth(), 2);
}

#[test]
fn parent_at_root_yields_none() {
    let p = ActorPath::root(Address::local("S"));
    assert!(p.parent().is_none());
}

#[test]
fn to_string_without_address_omits_prefix() {
    let p = ActorPath::root(Address::local("S")).child("user").child("foo");
    assert_eq!(p.to_string_without_address(), "/user/foo");
}

#[test]
fn paths_with_remote_address_display_remote_url() {
    let p = ActorPath::root(Address::remote("akka.tcp", "Sys", "host", 1)).child("user").child("a");
    assert_eq!(p.to_string(), "akka.tcp://Sys@host:1/user/a");
}

#[test]
fn equal_paths_are_equal() {
    let p1 = ActorPath::root(Address::local("S")).child("user").child("a");
    let p2 = ActorPath::root(Address::local("S")).child("user").child("a");
    assert_eq!(p1, p2);
}

#[test]
fn different_paths_are_unequal() {
    let p1 = ActorPath::root(Address::local("S")).child("user").child("a");
    let p2 = ActorPath::root(Address::local("S")).child("user").child("b");
    assert_ne!(p1, p2);
}
