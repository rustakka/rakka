// Reconnecting WebSocket hook for the `/ws` multiplex endpoint.

import { useEffect, useRef } from "react";

export interface TelemetryEvent {
  kind: string;
  [key: string]: unknown;
}

export interface WsOptions {
  topics?: string[];
  onEvent: (event: TelemetryEvent) => void;
  enabled?: boolean;
}

export function useTelemetryStream({
  topics,
  onEvent,
  enabled = true,
}: WsOptions): void {
  const cbRef = useRef(onEvent);
  cbRef.current = onEvent;

  useEffect(() => {
    if (!enabled) return;
    let closed = false;
    let socket: WebSocket | null = null;
    let attempt = 0;

    const connect = () => {
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      const query = topics?.length ? `?topics=${encodeURIComponent(topics.join(","))}` : "";
      const url = `${proto}://${window.location.host}/ws${query}`;
      socket = new WebSocket(url);
      socket.onopen = () => {
        attempt = 0;
      };
      socket.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data) as TelemetryEvent;
          cbRef.current(msg);
        } catch {
          // ignore non-JSON frames
        }
      };
      socket.onclose = () => {
        if (closed) return;
        attempt += 1;
        const delay = Math.min(30_000, 500 * 2 ** Math.min(attempt, 6));
        setTimeout(connect, delay);
      };
      socket.onerror = () => {
        socket?.close();
      };
    };

    connect();
    return () => {
      closed = true;
      socket?.close();
    };
  }, [enabled, topics?.join(",")]);
}
