//! `/ws` WebSocket multiplexer.
//!
//! Clients connect with an optional `?topics=a,b,c` query. The hub
//! forwards every [`rustakka_telemetry::bus::TelemetryEvent`] that
//! matches at least one topic. Connections also receive periodic ping
//! frames so browsers and load balancers that kill idle connections can
//! keep the stream alive.

use std::time::Duration;

use axum::extract::{Query, State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

use rustakka_telemetry::bus::{TelemetryBus, TelemetryEvent};

const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct WsHub {
    pub bus: TelemetryBus,
    pub channel_capacity: usize,
}

impl WsHub {
    pub fn new(bus: TelemetryBus, channel_capacity: usize) -> Self {
        Self { bus, channel_capacity }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct TopicFilter {
    /// Comma-separated topic list. Empty means "all topics".
    pub topics: Option<String>,
}

impl TopicFilter {
    pub fn matches(&self, event: &TelemetryEvent) -> bool {
        let Some(ref list) = self.topics else {
            return true;
        };
        if list.trim().is_empty() {
            return true;
        }
        let topic = event.topic();
        list.split(',').map(|s| s.trim()).any(|t| t == topic)
    }
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(filter): Query<TopicFilter>,
    State(hub): State<WsHub>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run_socket(socket, hub, filter))
}

async fn run_socket(socket: WebSocket, hub: WsHub, filter: TopicFilter) {
    let mut rx = hub.bus.subscribe();
    let (mut sink, mut stream) = socket.split();
    let mut heartbeat = tokio::time::interval(DEFAULT_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(e) => {
                        if !filter.matches(&e) {
                            continue;
                        }
                        let Ok(body) = serde_json::to_string(&e) else { continue };
                        if sink.send(Message::Text(body)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let msg = serde_json::json!({
                            "kind": "lagged", "skipped": skipped,
                        });
                        let _ = sink.send(Message::Text(msg.to_string())).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = heartbeat.tick() => {
                if sink.send(Message::Ping(Vec::new())).await.is_err() {
                    break;
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Text(_))) | Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Ping(_))) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
}
