//! [`SerialTransport`] — a `Transport` over a USB-attached serial port.
//!
//! Symmetric across both ends of the cable: `listen()` and
//! `associate()` both end up opening the configured device path. There
//! is exactly one peer per cable, so a single bidirectional link
//! carries every PDU. Re-attached cables and gadget reboots are handled
//! inside the transport via [`ReconnectPolicy`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio_serial::{SerialPortBuilderExt, SerialStream};

use atomr_core::actor::Address;
use atomr_remote::codec::{read_frame, write_frame};
use atomr_remote::pdu::{AkkaPdu, AssociateInfo, DisassociateReason};
use atomr_remote::transport::{InboundFrame, Transport, TransportError};

use crate::reconnect::ReconnectPolicy;

const DEFAULT_BAUD: u32 = 115_200;
const DEFAULT_MAX_FRAME: usize = 4 * 1024 * 1024;

/// Frame-oriented `Transport` over a USB-derived serial endpoint.
pub struct SerialTransport {
    system_name: String,
    device: PathBuf,
    baud: u32,
    max_frame_size: usize,
    state: Arc<SharedState>,
    inbound_tx: mpsc::UnboundedSender<InboundFrame>,
    inbound_rx: Mutex<Option<mpsc::UnboundedReceiver<InboundFrame>>>,
    shutdown: Arc<Notify>,
    reconnect_policy: ReconnectPolicy,
}

/// State shared between the public API and the background tasks.
struct SharedState {
    /// Outbound mpsc to the active link's writer task. `None` while the
    /// link is down (during initial connect or between reconnect
    /// attempts).
    sender: Mutex<Option<mpsc::UnboundedSender<AkkaPdu>>>,
    /// Address we advertise to the peer in our `Associate` PDU. Set
    /// once by `listen()`.
    local_address: Mutex<Option<Address>>,
    /// Address the peer claimed in their first `Associate` PDU. Used
    /// as `from:` for inbound frames once known.
    peer_address: Mutex<Option<Address>>,
}

impl SerialTransport {
    /// Open `device` on a `system_name`-tagged transport with the
    /// default baud rate (115200) and 4 MiB max frame size.
    pub fn new(system_name: impl Into<String>, device: impl Into<PathBuf>) -> Self {
        Self::with_options(system_name, device, DEFAULT_BAUD, DEFAULT_MAX_FRAME, ReconnectPolicy::default())
    }

    /// Construct with explicit baud, max frame size, and reconnect
    /// policy. Baud is ignored for true USB CDC-ACM endpoints (the
    /// rate is set by the link, not the user) but retained for true
    /// UARTs over USB-to-serial dongles.
    pub fn with_options(
        system_name: impl Into<String>,
        device: impl Into<PathBuf>,
        baud: u32,
        max_frame_size: usize,
        reconnect_policy: ReconnectPolicy,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            system_name: system_name.into(),
            device: device.into(),
            baud,
            max_frame_size,
            state: Arc::new(SharedState {
                sender: Mutex::new(None),
                local_address: Mutex::new(None),
                peer_address: Mutex::new(None),
            }),
            inbound_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            shutdown: Arc::new(Notify::new()),
            reconnect_policy,
        }
    }

    /// The local `Address` returned by `listen()`. `None` until then.
    pub fn local_address(&self) -> Option<Address> {
        self.state.local_address.lock().clone()
    }

    /// Drive the transport over caller-supplied byte-stream halves
    /// instead of opening a serial device. Useful for testing (with
    /// [`tokio::io::duplex`]) and for layering the Akka protocol over
    /// custom byte pipes (Unix sockets, SSH-tunneled streams, raw fds
    /// from external tools). No reconnect is attempted; if the streams
    /// close, the transport stays closed until shutdown.
    pub fn with_streams<R, W>(
        system_name: impl Into<String>,
        reader: R,
        writer: W,
        max_frame_size: usize,
    ) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let state = Arc::new(SharedState {
            sender: Mutex::new(None),
            local_address: Mutex::new(None),
            peer_address: Mutex::new(None),
        });
        let shutdown = Arc::new(Notify::new());
        let this = Self {
            system_name: system_name.into(),
            device: PathBuf::from("<streams>"),
            baud: DEFAULT_BAUD,
            max_frame_size,
            state: state.clone(),
            inbound_tx: tx.clone(),
            inbound_rx: Mutex::new(Some(rx)),
            shutdown: shutdown.clone(),
            reconnect_policy: ReconnectPolicy::never(),
        };
        let address = Address::remote("akka.serial", &this.system_name, "<streams>", 0);
        *state.local_address.lock() = Some(address);

        // Pre-create the outbound channel so send() works immediately
        // — the link runner takes the rx half.
        let (out_tx, out_rx) = mpsc::unbounded_channel::<AkkaPdu>();
        *state.sender.lock() = Some(out_tx);

        tokio::spawn(run_link_halves_with_outbound(
            reader,
            writer,
            out_rx,
            max_frame_size,
            state,
            tx,
            shutdown,
        ));
        this
    }
}

#[async_trait]
impl Transport for SerialTransport {
    async fn listen(&self) -> Result<Address, TransportError> {
        let device_str = self.device.to_string_lossy().into_owned();
        let address = Address::remote("akka.serial", &self.system_name, device_str, 0);
        *self.state.local_address.lock() = Some(address.clone());

        // Spawn the supervisor that owns the open/reader/writer/reconnect
        // lifecycle. It will retry until shutdown if the device isn't
        // present yet — that's expected when the gadget side boots
        // before the host side or vice versa.
        spawn_supervisor(
            self.device.clone(),
            self.baud,
            self.max_frame_size,
            self.state.clone(),
            self.inbound_tx.clone(),
            self.shutdown.clone(),
            self.reconnect_policy.clone(),
        );
        Ok(address)
    }

    async fn associate(&self, _target: &Address) -> Result<(), TransportError> {
        // No-op: serial is one-peer-per-cable; the supervisor opens the
        // device on `listen()` and keeps it open. The protocol layer
        // will hand us frames to send via `send()`; if the link is
        // currently down those return `Closed` and the protocol layer
        // retries.
        Ok(())
    }

    async fn send(&self, _target: &Address, pdu: AkkaPdu) -> Result<(), TransportError> {
        let sender = self.state.sender.lock().clone();
        match sender {
            Some(tx) => tx.send(pdu).map_err(|_| TransportError::Closed),
            None => Err(TransportError::Closed),
        }
    }

    fn inbound(&self) -> mpsc::UnboundedReceiver<InboundFrame> {
        self.inbound_rx.lock().take().unwrap_or_else(|| {
            let (_tx, rx) = mpsc::unbounded_channel();
            rx
        })
    }

    async fn disassociate(&self, _target: &Address) -> Result<(), TransportError> {
        if let Some(tx) = self.state.sender.lock().clone() {
            let _ = tx.send(AkkaPdu::Disassociate(DisassociateReason::Normal));
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        self.shutdown.notify_waiters();
        *self.state.sender.lock() = None;
        Ok(())
    }
}

fn spawn_supervisor(
    device: PathBuf,
    baud: u32,
    max_frame: usize,
    state: Arc<SharedState>,
    inbound: mpsc::UnboundedSender<InboundFrame>,
    shutdown: Arc<Notify>,
    policy: ReconnectPolicy,
) {
    tokio::spawn(async move {
        let mut delay = policy.initial;
        loop {
            // Race the open against shutdown.
            let opened = tokio::select! {
                _ = shutdown.notified() => return,
                result = open_device(&device, baud) => result,
            };
            match opened {
                Ok(stream) => {
                    delay = policy.initial;
                    run_link(stream, max_frame, state.clone(), inbound.clone(), shutdown.clone()).await;
                    if !policy.is_enabled() {
                        return;
                    }
                    tracing::warn!(device = %device.display(), "serial link dropped, reconnecting");
                }
                Err(e) => {
                    if !policy.is_enabled() {
                        tracing::warn!(device = %device.display(), error = %e, "serial open failed, reconnect disabled");
                        return;
                    }
                    tracing::debug!(device = %device.display(), error = %e, "serial open failed, will retry");
                }
            }

            tokio::select! {
                _ = shutdown.notified() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            delay = policy.next_delay(delay.max(Duration::from_millis(1)));
        }
    });
}

async fn open_device(device: &Path, baud: u32) -> Result<SerialStream, std::io::Error> {
    tokio_serial::new(device.to_string_lossy(), baud).open_native_async().map_err(io_from_serial)
}

fn io_from_serial(e: tokio_serial::Error) -> std::io::Error {
    match e.kind {
        tokio_serial::ErrorKind::NoDevice => std::io::Error::new(std::io::ErrorKind::NotFound, e.description),
        tokio_serial::ErrorKind::InvalidInput => {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e.description)
        }
        _ => std::io::Error::other(e.description),
    }
}

async fn run_link(
    stream: SerialStream,
    max_frame: usize,
    state: Arc<SharedState>,
    inbound: mpsc::UnboundedSender<InboundFrame>,
    shutdown: Arc<Notify>,
) {
    let (reader, writer) = tokio::io::split(stream);
    run_link_halves(reader, writer, max_frame, state, inbound, shutdown).await
}

async fn run_link_halves<R, W>(
    reader: R,
    writer: W,
    max_frame: usize,
    state: Arc<SharedState>,
    inbound: mpsc::UnboundedSender<InboundFrame>,
    shutdown: Arc<Notify>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<AkkaPdu>();
    *state.sender.lock() = Some(tx);
    run_link_halves_with_outbound(reader, writer, rx, max_frame, state, inbound, shutdown).await
}

async fn run_link_halves_with_outbound<R, W>(
    mut reader: R,
    mut writer: W,
    mut rx: mpsc::UnboundedReceiver<AkkaPdu>,
    max_frame: usize,
    state: Arc<SharedState>,
    inbound: mpsc::UnboundedSender<InboundFrame>,
    shutdown: Arc<Notify>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    // Writer task — drains the outbound mpsc onto the wire.
    let writer_task = tokio::spawn(async move {
        while let Some(pdu) = rx.recv().await {
            if write_frame(&mut writer, &pdu, max_frame).await.is_err() {
                break;
            }
            if matches!(pdu, AkkaPdu::Disassociate(_)) {
                let _ = writer.shutdown().await;
                break;
            }
        }
    });

    // Reader task — first frame must be an Associate so we learn the
    // peer's Address; thereafter we attribute every frame to it.
    let state_for_reader = state.clone();
    let inbound_for_reader = inbound.clone();
    let shutdown_for_reader = shutdown.clone();
    let reader_task = tokio::spawn(async move {
        loop {
            let read = tokio::select! {
                _ = shutdown_for_reader.notified() => break,
                r = read_frame(&mut reader, max_frame) => r,
            };
            let pdu = match read {
                Ok(p) => p,
                Err(_) => break,
            };

            // Stamp `from:` based on the peer's advertised Address.
            // Until the peer's Associate arrives, fall back to the
            // local Address — the protocol layer will treat the
            // frame as informational; the first Associate fixes
            // attribution for subsequent frames.
            let from = if let AkkaPdu::Associate(AssociateInfo { origin, .. }) = &pdu {
                *state_for_reader.peer_address.lock() = Some(origin.clone());
                origin.clone()
            } else {
                state_for_reader
                    .peer_address
                    .lock()
                    .clone()
                    .or_else(|| state_for_reader.local_address.lock().clone())
                    .unwrap_or_else(|| Address::local("?"))
            };

            if inbound_for_reader.send(InboundFrame { from, pdu }).is_err() {
                break;
            }
        }
    });

    let _ = tokio::join!(writer_task, reader_task);
    *state.sender.lock() = None;
    *state.peer_address.lock() = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_address_fields_round_trip_through_parse() {
        let path = "/dev/ttyACM0";
        let addr = Address::remote("akka.serial", "Sys", path, 0);
        let rendered = addr.to_string();
        let parsed = Address::parse(&rendered).expect("parse");
        assert_eq!(parsed, addr);
        assert_eq!(parsed.host.as_deref(), Some(path));
        assert_eq!(parsed.port, Some(0));
    }
}
