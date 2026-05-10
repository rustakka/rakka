//! Reactive loop driving everything that's not an actor: stdin →
//! outbound chat, periodic ping ticker, periodic stats overlay,
//! inbound dispatch.
//!
//! The `Peer` actor pushes every received `LinkMsg` into the channel
//! we own here; we route Chat/Ping/Pong appropriately and print
//! incoming chat lines with `[in]`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::watch;

use atomr_core::actor::ActorRef;
use atomr_remote::RemoteSystem;

use crate::peer::LinkMsg;
use crate::stats::Stats;

/// Driver inputs. Borrowed by the loop tasks; cheap to construct
/// because everything inside is `Arc`/`Clone`.
pub struct LoopArgs {
    pub remote: Arc<RemoteSystem>,
    pub stats: Arc<Stats>,
    /// `remote.local_address` rendered as a string. Embedded in
    /// outgoing Pings so the responder can reach us back.
    pub my_addr: String,
    /// `Some` on the connect side (came from `--peer`); `None` on the
    /// listen side until we learn it from the first incoming Ping.
    pub peer_addr_initial: Option<String>,
    pub ping_interval: Duration,
    pub stats_interval: Duration,
}

pub async fn run(mut inbound_rx: UnboundedReceiver<LinkMsg>, args: LoopArgs) -> Result<()> {
    let (peer_tx, peer_rx) = watch::channel(args.peer_addr_initial.clone());
    let peer_ref: Arc<Mutex<Option<ActorRef<LinkMsg>>>> = Arc::new(Mutex::new(None));

    // Inbound dispatch.
    let inbound_handle = {
        let remote = args.remote.clone();
        let stats = args.stats.clone();
        let peer_tx = peer_tx.clone();
        let peer_ref = peer_ref.clone();
        let my_addr = args.my_addr.clone();
        tokio::spawn(async move {
            while let Some(msg) = inbound_rx.recv().await {
                match msg {
                    LinkMsg::Chat { body } => {
                        println!("[in]  {body}");
                    }
                    LinkMsg::Ping { seq, sent_at_micros, from_addr } => {
                        // Learn the peer's address if we didn't already
                        // know it (listen side).
                        if peer_tx.borrow().is_none() {
                            let _ = peer_tx.send(Some(from_addr.clone()));
                        }
                        if let Some(target) = ensure_peer_ref(&remote, &peer_ref, &from_addr) {
                            target.tell(LinkMsg::Pong { seq, sent_at_micros });
                        }
                    }
                    LinkMsg::Pong { seq: _, sent_at_micros } => {
                        let now = now_micros();
                        let rtt = Duration::from_micros(now.saturating_sub(sent_at_micros));
                        stats.record_recv(rtt);
                    }
                }
            }
            tracing::debug!(my_addr = %my_addr, "inbound channel closed");
        })
    };

    // Outbound stdin → Chat.
    let stdin_handle = {
        let remote = args.remote.clone();
        let peer_rx = peer_rx.clone();
        let peer_ref = peer_ref.clone();
        tokio::spawn(async move {
            let stdin = tokio::io::stdin();
            let mut lines = BufReader::new(stdin).lines();
            loop {
                tokio::select! {
                    line = lines.next_line() => {
                        match line {
                            Ok(Some(body)) => {
                                let Some(addr) = peer_rx.borrow().clone() else {
                                    eprintln!("[..]  peer not associated yet, dropping line");
                                    continue;
                                };
                                if let Some(target) = ensure_peer_ref(&remote, &peer_ref, &addr) {
                                    target.tell(LinkMsg::Chat { body: body.clone() });
                                    println!("[out] {body}");
                                }
                            }
                            Ok(None) => break,           // EOF on stdin
                            Err(e) => {
                                tracing::warn!(error = %e, "stdin read error");
                                break;
                            }
                        }
                    }
                }
            }
        })
    };

    // Ping ticker.
    let ping_handle = {
        let remote = args.remote.clone();
        let stats = args.stats.clone();
        let mut peer_rx = peer_rx.clone();
        let peer_ref = peer_ref.clone();
        let my_addr = args.my_addr.clone();
        let interval = args.ping_interval;
        tokio::spawn(async move {
            let mut seq: u64 = 0;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(addr) = peer_rx.borrow_and_update().clone() else {
                    // Wait until we know the peer.
                    if peer_rx.changed().await.is_err() {
                        break;
                    }
                    continue;
                };
                if let Some(target) = ensure_peer_ref(&remote, &peer_ref, &addr) {
                    let sent_at = now_micros();
                    target.tell(LinkMsg::Ping { seq, sent_at_micros: sent_at, from_addr: my_addr.clone() });
                    stats.record_sent();
                    seq = seq.wrapping_add(1);
                }
            }
        })
    };

    // Stats overlay.
    let stats_handle = {
        let stats = args.stats.clone();
        let interval = args.stats_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate-first tick so we don't print 0/0 before any
            // ping has had a chance to land.
            tick.tick().await;
            loop {
                tick.tick().await;
                let snap = stats.snapshot();
                let fmt_ms = |d: Option<Duration>| {
                    d.map(|d| format!("{:.1}ms", d.as_secs_f64() * 1000.0)).unwrap_or_else(|| "—".into())
                };
                println!(
                    "stats: sent={} recvd={} loss={:.1}% p50={} p95={} p99={}",
                    snap.sent,
                    snap.recvd,
                    snap.loss_pct,
                    fmt_ms(snap.p50),
                    fmt_ms(snap.p95),
                    fmt_ms(snap.p99),
                );
            }
        })
    };

    // Wait for Ctrl-C; cancel everything else.
    tokio::signal::ctrl_c().await?;
    println!("(ctrl-c received, shutting down)");
    inbound_handle.abort();
    stdin_handle.abort();
    ping_handle.abort();
    stats_handle.abort();
    Ok(())
}

fn ensure_peer_ref(
    remote: &Arc<RemoteSystem>,
    cell: &Arc<Mutex<Option<ActorRef<LinkMsg>>>>,
    addr: &str,
) -> Option<ActorRef<LinkMsg>> {
    let mut guard = cell.lock();
    if let Some(r) = guard.as_ref() {
        return Some(r.clone());
    }
    let path = format!("{addr}/user/peer");
    match remote.actor_selection::<LinkMsg>(&path) {
        Some(r) => {
            *guard = Some(r.clone());
            Some(r)
        }
        None => {
            tracing::warn!(path = %path, "actor_selection returned None");
            None
        }
    }
}

fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}
