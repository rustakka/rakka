//! Per-association `Endpoint` actor.
//! akka.net: `Remote/EndpointWriter.cs`, `Remote/EndpointReader.cs`,
//! `Remote/Endpoint.cs`.
//!
//! Each endpoint owns one peer (`Address` + UID). The writer half pumps
//! outbound user/system payloads (with heartbeats when idle); the reader
//! half consumes inbound payloads dispatched by the `EndpointManager`
//! and routes them up to the local `ActorSystem`. Both halves cooperate
//! with the [`AckedSendBuffer`] / [`AckedReceiveBuffer`] for
//! sliding-window reliable delivery.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Notify};

use atomr_core::actor::Address;

use crate::acked_delivery::{AckedReceiveBuffer, AckedSendBuffer, SeqNo};
use crate::envelope::RemoteEnvelope;
use crate::pdu::{AkkaPdu, DisassociateReason};
use crate::settings::RemoteSettings;
use crate::transport::AkkaProtocolTransport;

/// Inbound payload destined for the local `ActorSystem`. Produced by the
/// reader half and consumed by the local dispatcher (set on the
/// provider).
#[derive(Debug)]
pub struct InboundEnvelope {
    pub envelope: RemoteEnvelope,
}

/// Outbound commands accepted by the writer half.
#[derive(Debug)]
pub enum EndpointCmd {
    Send(RemoteEnvelope),
    SendSystem(RemoteEnvelope),
    /// Resend the buffered window after a reconnect.
    ResendBuffer,
    /// Apply an inbound `Ack` to the send buffer.
    ApplyAck(crate::pdu::AckInfo),
    /// Drain and disassociate.
    Shutdown(DisassociateReason),
}

/// PDUs the manager dispatches to this endpoint's reader half.
#[derive(Debug)]
pub enum InboundPdu {
    Payload(RemoteEnvelope),
    Ack(crate::pdu::AckInfo),
}

#[derive(Clone)]
pub struct EndpointHandle {
    pub remote: Address,
    pub remote_uid: u64,
    cmd_tx: mpsc::UnboundedSender<EndpointCmd>,
    pdu_tx: mpsc::UnboundedSender<InboundPdu>,
    shutdown: Arc<Notify>,
}

impl EndpointHandle {
    pub fn send(&self, env: RemoteEnvelope) {
        let _ = self.cmd_tx.send(EndpointCmd::Send(env));
    }
    pub fn send_system(&self, env: RemoteEnvelope) {
        let _ = self.cmd_tx.send(EndpointCmd::SendSystem(env));
    }
    pub fn resend(&self) {
        let _ = self.cmd_tx.send(EndpointCmd::ResendBuffer);
    }
    pub fn apply_ack(&self, ack: crate::pdu::AckInfo) {
        let _ = self.cmd_tx.send(EndpointCmd::ApplyAck(ack));
    }
    /// Hand off an inbound PDU (called by the manager dispatch task).
    pub fn deliver(&self, pdu: InboundPdu) {
        let _ = self.pdu_tx.send(pdu);
    }
    pub fn shutdown(&self, reason: DisassociateReason) {
        let _ = self.cmd_tx.send(EndpointCmd::Shutdown(reason));
        self.shutdown.notify_waiters();
    }
}

/// Spawn an endpoint reader/writer pair. The returned handle is what
/// `RemoteActorRef::tell_serialized` ultimately writes to.
pub fn spawn_endpoint(
    protocol: Arc<AkkaProtocolTransport>,
    settings: RemoteSettings,
    remote: Address,
    remote_uid: u64,
    inbound_sink: mpsc::UnboundedSender<InboundEnvelope>,
) -> EndpointHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<EndpointCmd>();
    let (pdu_tx, pdu_rx) = mpsc::unbounded_channel::<InboundPdu>();
    let shutdown = Arc::new(Notify::new());

    let cmd_tx_for_reader = cmd_tx.clone();
    let writer = EndpointWriter {
        protocol: protocol.clone(),
        settings: settings.clone(),
        remote: remote.clone(),
        remote_uid,
        seq: SeqNo::default(),
        send_buf: AckedSendBuffer::new(settings.ack_window),
        cmd_rx,
        shutdown: shutdown.clone(),
    };
    let reader = EndpointReader {
        remote: remote.clone(),
        recv_buf: AckedReceiveBuffer::new(),
        inbound_sink,
        pdu_rx,
        cmd_tx: cmd_tx_for_reader,
        protocol: protocol.clone(),
        shutdown: shutdown.clone(),
    };

    tokio::spawn(writer.run());
    tokio::spawn(reader.run());

    EndpointHandle { remote, remote_uid, cmd_tx, pdu_tx, shutdown }
}

struct EndpointWriter {
    protocol: Arc<AkkaProtocolTransport>,
    settings: RemoteSettings,
    remote: Address,
    #[allow(dead_code)]
    remote_uid: u64,
    seq: SeqNo,
    send_buf: AckedSendBuffer,
    cmd_rx: mpsc::UnboundedReceiver<EndpointCmd>,
    shutdown: Arc<Notify>,
}

impl EndpointWriter {
    async fn run(mut self) {
        let mut heartbeat = tokio::time::interval(self.settings.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = heartbeat.tick().await;

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    let _ = self.protocol.send_pdu(
                        &self.remote,
                        AkkaPdu::Disassociate(DisassociateReason::Normal),
                    ).await;
                    break;
                }
                cmd = self.cmd_rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    match cmd {
                        EndpointCmd::Send(mut env) | EndpointCmd::SendSystem(mut env) => {
                            env.seq_no = self.seq.advance();
                            let _ = self.send_buf.push(env.clone());
                            if let Err(e) = self
                                .protocol
                                .send_pdu(&self.remote, AkkaPdu::Payload(env))
                                .await
                            {
                                tracing::warn!(remote = %self.remote, "send failed: {e}");
                            }
                        }
                        EndpointCmd::ApplyAck(ack) => {
                            self.send_buf.apply_ack(&ack);
                        }
                        EndpointCmd::ResendBuffer => {
                            let envs = self.send_buf.drain_resend();
                            for e in envs {
                                let _ = self
                                    .protocol
                                    .send_pdu(&self.remote, AkkaPdu::Payload(e))
                                    .await;
                            }
                        }
                        EndpointCmd::Shutdown(reason) => {
                            let _ = self
                                .protocol
                                .send_pdu(&self.remote, AkkaPdu::Disassociate(reason))
                                .await;
                            break;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let _ = self
                        .protocol
                        .send_pdu(&self.remote, AkkaPdu::Heartbeat)
                        .await;
                }
            }
        }
    }
}

struct EndpointReader {
    remote: Address,
    recv_buf: AckedReceiveBuffer,
    inbound_sink: mpsc::UnboundedSender<InboundEnvelope>,
    pdu_rx: mpsc::UnboundedReceiver<InboundPdu>,
    cmd_tx: mpsc::UnboundedSender<EndpointCmd>,
    protocol: Arc<AkkaProtocolTransport>,
    shutdown: Arc<Notify>,
}

impl EndpointReader {
    async fn run(mut self) {
        let mut ack_tick = tokio::time::interval(Duration::from_millis(200));
        ack_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = ack_tick.tick().await;

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => break,
                pdu = self.pdu_rx.recv() => {
                    let Some(pdu) = pdu else { break };
                    match pdu {
                        InboundPdu::Payload(env) => {
                            if self.recv_buf.observe(env.seq_no) {
                                let _ = self.inbound_sink.send(InboundEnvelope { envelope: env });
                            }
                        }
                        InboundPdu::Ack(ack) => {
                            let _ = self.cmd_tx.send(EndpointCmd::ApplyAck(ack));
                        }
                    }
                }
                _ = ack_tick.tick() => {
                    let ack = self.recv_buf.ack();
                    if ack.cumulative_ack > 0 {
                        let _ = self
                            .protocol
                            .send_pdu(&self.remote, AkkaPdu::Ack(ack))
                            .await;
                    }
                }
            }
        }
    }
}
