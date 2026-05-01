//! `rakka-remote`. akka.net: `src/core/Akka.Remote/`.
//!
//! Cross-process actor remoting for `rakka`. Two `ActorSystem`s on
//! different machines (or different ports on the same machine) become
//! reachable from each other once each side has called
//! [`RemoteSystem::start`] with overlapping codecs.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use rakka_core::prelude::*;
//! use rakka_remote::{RemoteSettings, RemoteSystem};
//!
//! let sys_a = ActorSystem::create("A", rakka_config::Config::reference()).await?;
//! let remote_a = RemoteSystem::start(sys_a.clone(), "127.0.0.1:0".parse()?, RemoteSettings::default()).await?;
//! remote_a.register_bincode::<String>();
//! # Ok(()) }
//! ```
//!
//! ## Module map
//!
//! | Akka.NET | rakka |
//! |---|---|
//! | `Akka.Remote.RemoteSettings` | [`settings::RemoteSettings`] |
//! | `Akka.Remote.AddressUidExtension` | [`address_uid::AddressUid`] |
//! | `Akka.Remote.Transport.Transport` | [`transport::Transport`] |
//! | `Akka.Remote.Transport.DotNetty.TcpTransport` | [`transport::TcpTransport`] |
//! | `Akka.Remote.Transport.AkkaProtocolTransport` | [`transport::AkkaProtocolTransport`] |
//! | `Akka.Remote.Transport.ThrottleTransportAdapter` | [`transport::ThrottleTransport`] |
//! | `Akka.Remote.Transport.FailureInjectorTransportAdapter` | [`transport::FailureInjectorTransport`] |
//! | `Akka.Remote.Transport.TestTransport` | [`transport::TestTransport`] |
//! | `Akka.Remote.EndpointManager` | [`endpoint_manager::EndpointManager`] |
//! | `Akka.Remote.Endpoint` (Reader+Writer) | [`endpoint::EndpointHandle`] |
//! | `Akka.Remote.Transport.AckedDelivery` | [`acked_delivery`] |
//! | `Akka.Remote.RemoteActorRef` | [`remote_ref::RemoteActorRefImpl`] |
//! | `Akka.Remote.RemoteActorRefProvider` | [`provider::RemoteActorRefProvider`] |
//! | `Akka.Remote.RemoteSystemDaemon` | [`system_daemon::RemoteSystemDaemon`] |
//! | `Akka.Remote.RemoteDeployer` | [`system_daemon::RemoteDeployer`] |
//! | `Akka.Remote.RemoteWatcher` | [`remote_watcher::RemoteWatcher`] |
//! | `Akka.Remote.RemoteMetricsExtension` | [`metrics::RemoteMetrics`] |
//! | `Akka.Remote.DefaultFailureDetectorRegistry` | [`failure_detector_registry::FailureDetectorRegistry`] |
//! | `Akka.Remote.Routing.RemoteRouterConfig` | [`router::RemoteRouterConfig`] |

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod acked_delivery;
pub mod cache;
pub mod chunking;
pub mod error;
pub mod reader_writer;
pub mod remote_props;
pub mod tls;
pub mod address_uid;
pub mod codec;
pub mod deadline_detector;
pub mod endpoint;
pub mod endpoint_manager;
pub mod envelope;
pub mod failure_detector;
pub mod failure_detector_registry;
pub mod metrics;
pub mod pdu;
pub mod phi_accrual;
pub mod provider;
pub mod remote_ref;
pub mod remote_watcher;
pub mod router;
pub mod serialization;
pub mod settings;
pub mod system;
pub mod system_daemon;
pub mod transport;

pub use address_uid::AddressUid;
pub use deadline_detector::DeadlineFailureDetector;
pub use endpoint::{EndpointHandle, InboundEnvelope};
pub use endpoint_manager::{AssociationState, EndpointManager};
pub use cache::LruCache;
pub use chunking::{Chunk, ChunkError, Chunker, Reassembler};
pub use error::{RemoteError, RemoteErrorKind};
pub use reader_writer::{spawn_reader_writer, RawTransport, ReaderWriterHandle};
pub use tls::{parse_pem_blocks, TlsConfig, TlsError};
pub use remote_props::{register_bincode as register_remote_props, RemotePropsError, RemotePropsRegistry};
pub use envelope::RemoteEnvelope;
pub use failure_detector::FailureDetector;
pub use failure_detector_registry::FailureDetectorRegistry;
pub use metrics::{RemoteMetrics, RemoteMetricsRow, RemoteMetricsSnapshot};
pub use pdu::{AckInfo, AkkaPdu, AssociateInfo, DisassociateReason, PROTOCOL_VERSION};
pub use phi_accrual::PhiAccrualFailureDetector;
pub use provider::RemoteActorRefProvider;
pub use remote_ref::RemoteActorRefImpl;
pub use remote_watcher::RemoteWatcher;
pub use router::{RemoteRouterConfig, RemoteRouterStrategy};
pub use serialization::{
    SerializeError, SerializerRegistry, TypeCodec, BINCODE_SERIALIZER_ID, JSON_SERIALIZER_ID,
    SYSTEM_SERIALIZER_ID,
};
pub use settings::RemoteSettings;
pub use system::RemoteSystem;
pub use system_daemon::{LocalDispatch, RemoteDeployer, RemoteSystemDaemon};
pub use transport::{
    AkkaProtocolTransport, FailureInjectorTransport, InboundFrame, InjectionMode,
    TcpTransport, TestTransport, ThrottleMode, ThrottleTransport, Transport, TransportError,
};
