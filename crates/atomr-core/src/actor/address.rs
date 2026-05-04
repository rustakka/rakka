//! An `Address` locates an actor system on the network.
//! akka.net: `Actor/Address.cs`.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Address {
    pub protocol: String,
    pub system: String,
    pub host: Option<String>,
    pub port: Option<u16>,
}

impl Address {
    pub fn local(system: impl Into<String>) -> Self {
        Self { protocol: "akka".into(), system: system.into(), host: None, port: None }
    }

    pub fn remote(
        protocol: impl Into<String>,
        system: impl Into<String>,
        host: impl Into<String>,
        port: u16,
    ) -> Self {
        Self { protocol: protocol.into(), system: system.into(), host: Some(host.into()), port: Some(port) }
    }

    pub fn has_local_scope(&self) -> bool {
        self.host.is_none()
    }

    pub fn has_global_scope(&self) -> bool {
        self.host.is_some()
    }

    /// Parses strings like `akka://sys` or `akka.tcp://sys@host:1234`.
    pub fn parse(s: &str) -> Option<Self> {
        let (protocol, rest) = s.split_once("://")?;
        if let Some((sys, host_port)) = rest.split_once('@') {
            let (host, port) = host_port.split_once(':')?;
            Some(Self::remote(protocol, sys, host, port.parse().ok()?))
        } else {
            Some(Self::local(rest).with_protocol(protocol))
        }
    }

    fn with_protocol(mut self, p: impl Into<String>) -> Self {
        self.protocol = p.into();
        self
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.host, self.port) {
            (Some(h), Some(p)) => write!(f, "{}://{}@{}:{}", self.protocol, self.system, h, p),
            _ => write!(f, "{}://{}", self.protocol, self.system),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_has_no_host() {
        let a = Address::local("Sys");
        assert!(a.has_local_scope());
        assert_eq!(a.to_string(), "akka://Sys");
    }

    #[test]
    fn remote_roundtrips() {
        let a = Address::remote("akka.tcp", "Sys", "host", 1234);
        assert!(a.has_global_scope());
        assert_eq!(Address::parse(&a.to_string()).unwrap(), a);
    }

    #[test]
    fn parses_local_form() {
        assert_eq!(Address::parse("akka://Sys").unwrap(), Address::local("Sys"));
    }
}
