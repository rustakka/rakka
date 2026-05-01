//! rakka-cluster. akka.net: `src/core/Akka.Cluster/`.
//!
//! Contains membership, gossip, reachability, heartbeat, and the
//! split-brain resolver strategies.

mod events;
mod gossip;
mod gossip_pdu;
mod heartbeat;
mod heartbeat_sender;
mod leader;
mod member;
mod multi_dc;
mod membership;
mod reachability;
mod remote_adapter;
mod sbr;
mod sbr_runtime;
mod vector_clock;

pub use events::{ClusterEvent, ClusterEventBus, SubscriptionHandle};
pub use gossip::{Gossip, GossipOverview};
pub use gossip_pdu::{decide as gossip_decide, pick_gossip_target, GossipDecision, GossipPdu};
pub use heartbeat::HeartbeatState;
pub use heartbeat_sender::{HeartbeatSender, PeerHeartbeat};
pub use leader::{elect_leader, is_converged, next_status};
pub use member::{Member, MemberStatus};
pub use multi_dc::{
    heartbeat_interval_for, member_dc, partition_by_dc, same_dc, CrossDcSettings, DC_ROLE_PREFIX,
    DEFAULT_DC,
};
pub use membership::MembershipState;
pub use reachability::{Reachability, ReachabilityStatus};
pub use remote_adapter::ClusterRemoteAdapter;
pub use sbr::{
    DowningDecision, DowningStrategy, KeepMajorityStrategy, KeepOldestStrategy, KeepReferee,
    LeaseMajorityStrategy, SplitBrainResolver, StaticQuorumStrategy,
};
pub use sbr_runtime::{SbrAction, SbrRuntime};
pub use vector_clock::{VectorClock, VectorRelation};
