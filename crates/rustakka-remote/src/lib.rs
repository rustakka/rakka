//! rustakka-remote. akka.net: `src/core/Akka.Remote/`.
//!
//! * [`Transport`] — pluggable transport trait.
//! * [`TcpTransport`] — default TCP transport with length-prefixed frames.
//! * [`Endpoint`] — outbound association handling.
//! * [`PhiAccrualFailureDetector`] / [`DeadlineFailureDetector`].

mod deadline_detector;
mod endpoint;
mod envelope;
mod failure_detector;
mod phi_accrual;
mod registry;
mod tcp_transport;
mod transport;

pub use deadline_detector::DeadlineFailureDetector;
pub use endpoint::{Endpoint, EndpointManager};
pub use envelope::RemoteEnvelope;
pub use failure_detector::FailureDetector;
pub use phi_accrual::PhiAccrualFailureDetector;
pub use registry::EndpointRegistry;
pub use tcp_transport::TcpTransport;
pub use transport::{Transport, TransportError};
