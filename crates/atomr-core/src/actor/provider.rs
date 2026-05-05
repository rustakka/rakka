//! Actor ref providers.,
//! `Actor/LocalActorRefProvider.cs`.

use super::address::Address;
use super::path::ActorPath;

/// Abstract over which backing runtime provides an actor ref:
/// local, remote, or cluster. Default is `Local`.
pub trait ActorRefProvider: Send + Sync {
    fn root_path(&self) -> ActorPath;
    fn address(&self) -> &Address;
}

pub struct LocalActorRefProvider {
    pub address: Address,
}

impl LocalActorRefProvider {
    pub fn new(address: Address) -> Self {
        Self { address }
    }
}

impl ActorRefProvider for LocalActorRefProvider {
    fn root_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone())
    }

    fn address(&self) -> &Address {
        &self.address
    }
}
