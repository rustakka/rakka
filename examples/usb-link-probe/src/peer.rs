//! The wire-level message type and the `Peer` actor that hosts the
//! receive side of the link.
//!
//! Both endpoints register the same `LinkMsg` enum via bincode and
//! both run a `Peer` actor under `/user/peer`. The actor itself is a
//! thin shim — it forwards every received message into a tokio channel
//! that the io-loop drives. Keeping the reactive logic out of the
//! actor simplifies coordination with the stdin reader, ping ticker,
//! and stats ticker (which are not actors).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use atomr_core::actor::{Actor, Context};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LinkMsg {
    /// Operator-typed chat line. Prints with `[in]` on the receiver.
    Chat { body: String },
    /// Outbound liveness probe. `from_addr` is the sender's
    /// `remote.local_address` so the responder can reach back.
    Ping { seq: u64, sent_at_micros: u64, from_addr: String },
    /// Echo of a previous `Ping`. Receiver computes RTT against
    /// `sent_at_micros` and feeds the rolling stats window.
    Pong { seq: u64, sent_at_micros: u64 },
}

pub struct Peer {
    inbound: UnboundedSender<LinkMsg>,
}

impl Peer {
    pub fn new(inbound: UnboundedSender<LinkMsg>) -> Self {
        Self { inbound }
    }
}

#[async_trait]
impl Actor for Peer {
    type Msg = LinkMsg;

    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: LinkMsg) {
        // Drop on closed receiver — the io-loop has already exited and
        // we're racing shutdown. No point logging from inside the actor.
        let _ = self.inbound.send(msg);
    }
}
