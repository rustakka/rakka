//! `Deploy` and `Scope`. akka.net: `Actor/Deploy.cs`, `Actor/Scope.cs`.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deploy {
    pub path: Option<String>,
    pub dispatcher: Option<String>,
    pub mailbox: Option<String>,
    pub scope: Scope,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Scope {
    #[default]
    Local,
    Remote {
        address: String,
    },
}

impl Deploy {
    pub fn local() -> Self {
        Self::default()
    }

    pub fn remote(address: impl Into<String>) -> Self {
        Self { scope: Scope::Remote { address: address.into() }, ..Self::default() }
    }

    pub fn with_dispatcher(mut self, d: impl Into<String>) -> Self {
        self.dispatcher = Some(d.into());
        self
    }

    pub fn with_mailbox(mut self, m: impl Into<String>) -> Self {
        self.mailbox = Some(m.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_deploy_sets_scope() {
        let d = Deploy::remote("akka.tcp://S@host:1").with_dispatcher("dp");
        assert!(matches!(d.scope, Scope::Remote { .. }));
        assert_eq!(d.dispatcher.as_deref(), Some("dp"));
    }
}
