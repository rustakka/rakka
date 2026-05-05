//! `atomr-remote`.
//!
//! Cross-process actor remoting for `atomr`. Two `ActorSystem`s on
//! different machines (or different ports on the same machine) become
//! reachable from each other once each side has called
//! [`RemoteSystem::start`] with overlapping codecs.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use atomr_core::prelude::*;
//! use atomr_remote::{RemoteSettings, RemoteSystem};
//!
//! let sys_a = ActorSystem::create("A", atomr_config::Config::reference()).await?;
//! let remote_a = RemoteSystem::start(sys_a.clone(), "127.0.0.1:0".parse()?, RemoteSettings::default()).await?;
//! remote_a.register_bincode::<String>();
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod acked_delivery;
pub mod address_uid;
pub mod cache;
pub mod chunking;
pub mod codec;
pub mod deadline_detector;
pub mod endpoint;
pub mod endpoint_manager;
pub mod envelope;
pub mod error;
pub mod failure_detector;
pub mod failure_detector_registry;
pub mod metrics;
pub mod pdu;
pub mod phi_accrual;
pub mod provider;
pub mod reader_writer;
pub mod remote_props;
pub mod remote_ref;
pub mod remote_watcher;
pub mod router;
pub mod send_queue;
pub mod serialization;
pub mod settings;
pub mod system;
pub mod system_daemon;
pub mod tls;
pub mod transport;

pub use address_uid::AddressUid;
pub use cache::LruCache;
pub use chunking::{Chunk, ChunkError, Chunker, Reassembler};
pub use deadline_detector::DeadlineFailureDetector;
pub use endpoint::{EndpointHandle, InboundEnvelope};
pub use endpoint_manager::{AssociationState, EndpointManager};
pub use envelope::RemoteEnvelope;
pub use error::{RemoteError, RemoteErrorKind};
pub use failure_detector::FailureDetector;
pub use failure_detector_registry::FailureDetectorRegistry;
pub use metrics::{RemoteMetrics, RemoteMetricsRow, RemoteMetricsSnapshot};
pub use pdu::{AckInfo, AkkaPdu, AssociateInfo, DisassociateReason, PROTOCOL_VERSION};
pub use phi_accrual::PhiAccrualFailureDetector;
pub use provider::RemoteActorRefProvider;
pub use reader_writer::{spawn_reader_writer, RawTransport, ReaderWriterHandle};
pub use remote_props::{register_bincode as register_remote_props, RemotePropsError, RemotePropsRegistry};
pub use remote_ref::RemoteActorRefImpl;
pub use remote_watcher::RemoteWatcher;
pub use router::{RemoteRouterConfig, RemoteRouterStrategy};
pub use send_queue::{BoundedSendQueue, SendOutcome};
pub use serialization::{
    SerializeError, SerializerRegistry, TypeCodec, BINCODE_SERIALIZER_ID, JSON_SERIALIZER_ID,
    SYSTEM_SERIALIZER_ID,
};
pub use settings::{RemoteSettings, SendQueueOverflow};
pub use system::RemoteSystem;
pub use system_daemon::{LocalDispatch, RemoteDeployer, RemoteSystemDaemon};
pub use tls::{parse_pem_blocks, TlsConfig, TlsError};
pub use transport::{
    AkkaProtocolTransport, FailureInjectorTransport, InboundFrame, InjectionMode, TcpTransport,
    TestTransport, ThrottleMode, ThrottleTransport, Transport, TransportError,
};
