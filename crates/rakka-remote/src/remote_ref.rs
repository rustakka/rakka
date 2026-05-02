//! `RemoteActorRefImpl`. akka.net: `Remote/RemoteActorRef.cs`.
//!
//! Concrete implementation of `rakka_core::actor::RemoteRef` that
//! serializes outbound messages via the [`SerializerRegistry`] and ships
//! them through an [`EndpointManager`].

use rakka_core::actor::{ActorPath, RemoteRef, RemoteSystemMsg, SerializedMessage};

use crate::endpoint_manager::EndpointManager;
use crate::envelope::RemoteEnvelope;
use crate::serialization::{SerializerRegistry, SYSTEM_SERIALIZER_ID};

pub struct RemoteActorRefImpl {
    pub path: ActorPath,
    pub endpoint_manager: EndpointManager,
    pub registry: SerializerRegistry,
    /// Local `ActorSystem` UID, written into `sender_uid` for replies.
    pub local_uid: u64,
}

impl std::fmt::Debug for RemoteActorRefImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteActorRefImpl").field("path", &self.path.to_string()).finish()
    }
}

impl RemoteActorRefImpl {
    pub fn new(
        path: ActorPath,
        endpoint_manager: EndpointManager,
        registry: SerializerRegistry,
        local_uid: u64,
    ) -> Self {
        Self { path, endpoint_manager, registry, local_uid }
    }

    fn target_address(&self) -> rakka_core::actor::Address {
        self.path.address.clone()
    }
}

impl RemoteRef for RemoteActorRefImpl {
    fn path(&self) -> &ActorPath {
        &self.path
    }

    fn tell_serialized(&self, msg: SerializedMessage) {
        let env = RemoteEnvelope::user(
            self.path.to_string(),
            msg.sender.as_ref().map(|p| p.to_string()),
            self.local_uid,
            self.path.uid,
            0, // seq_no assigned by writer
            msg.serializer_id,
            msg.manifest,
            msg.payload,
        );
        let mgr = self.endpoint_manager.clone();
        let target = self.target_address();
        let metrics = mgr.metrics();
        let bytes = env.payload.len();
        tokio::spawn(async move {
            match mgr.endpoint_for(&target).await {
                Ok(handle) => {
                    metrics.record_send(&target, bytes);
                    handle.send(env);
                }
                Err(e) => {
                    metrics.record_error(&target);
                    tracing::warn!(target = %target, "remote tell failed: {e}");
                }
            }
        });
    }

    fn tell_system(&self, msg: RemoteSystemMsg) {
        // Encode the RemoteSystemMsg via the system serializer and send
        // it as a Payload PDU with `system = true`.
        let manifest = std::any::type_name::<RemoteSystemMsg>().to_string();
        let bytes = match self.registry.encode_typed(&msg) {
            Ok((_id, _m, b)) => b,
            Err(e) => {
                tracing::warn!("system msg encode failed: {e}");
                return;
            }
        };
        let env = RemoteEnvelope::system_msg(
            self.path.to_string(),
            self.local_uid,
            self.path.uid,
            0,
            manifest,
            bytes,
        );
        let mgr = self.endpoint_manager.clone();
        let target = self.target_address();
        let _ = SYSTEM_SERIALIZER_ID; // referenced for clarity
        tokio::spawn(async move {
            if let Ok(handle) = mgr.endpoint_for(&target).await {
                handle.send_system(env);
            }
        });
    }
}
