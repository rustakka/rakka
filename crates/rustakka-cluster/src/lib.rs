//! rustakka-cluster. akka.net: `src/core/Akka.Cluster/`.
//!
//! Contains membership, gossip, reachability, heartbeat, and the
//! split-brain resolver strategies.

mod gossip;
mod heartbeat;
mod member;
mod membership;
mod reachability;
mod sbr;
mod vector_clock;

pub use gossip::{Gossip, GossipOverview};
pub use heartbeat::HeartbeatState;
pub use member::{Member, MemberStatus};
pub use membership::MembershipState;
pub use reachability::{Reachability, ReachabilityStatus};
pub use sbr::{
    DowningStrategy, KeepMajorityStrategy, KeepOldestStrategy, KeepReferee, LeaseMajorityStrategy,
    SplitBrainResolver, StaticQuorumStrategy,
};
pub use vector_clock::{VectorClock, VectorRelation};
