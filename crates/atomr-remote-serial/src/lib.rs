//! Serial / USB-attached transport for atomr-remoting.
//!
//! `atomr-remote-serial` implements [`atomr_remote::transport::Transport`]
//! over a serial endpoint exposed by a USB CDC-ACM device — typically
//! `/dev/ttyACM0` (Linux host), `/dev/ttyGS0` (Linux gadget side),
//! `COMx` (Windows), or `/dev/cu.usbmodemXXXX` (macOS). The use case is
//! two hosts physically connected by a USB cable that want to exchange
//! actor messages without going over the network.
//!
//! ```no_run
//! use std::sync::Arc;
//! use atomr_remote::{RemoteSettings, RemoteSystem};
//! use atomr_remote_serial::SerialTransport;
//!
//! # async fn run(system: atomr_core::actor::ActorSystem,
//! #              settings: RemoteSettings)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! let transport = Arc::new(SerialTransport::new("SystemA", "/dev/ttyACM0"));
//! let _remote = RemoteSystem::start_with_transport(system, transport, settings).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See `docs/remoting.md` for the wiring on the gadget side and the
//! zero-code "USB-Ethernet" alternative (CDC-NCM + existing TCP transport).

mod reconnect;
mod transport;

pub use reconnect::ReconnectPolicy;
pub use transport::SerialTransport;
