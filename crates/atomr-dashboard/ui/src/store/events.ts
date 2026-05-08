// Zustand store for the live telemetry event stream. Keeps a bounded
// ring buffer of the most recent events so pages can do sparklines
// without going back to the server.
//
// `totalSeen` is a monotonically-increasing count of every event ever
// appended (i.e. *not* clamped by MAX_EVENTS). Subscribers that need to
// process "what's arrived since last time" should track this instead of
// `events.length` — the latter plateaus at MAX_EVENTS and can no longer
// signal that fresh events have arrived once the buffer is full.

import { create } from "zustand";
import type { TelemetryEvent } from "@/lib/ws";

const MAX_EVENTS = 500;

interface EventsState {
  events: TelemetryEvent[];
  totalSeen: number;
  append: (event: TelemetryEvent) => void;
  clear: () => void;
}

export const useEventsStore = create<EventsState>((set) => ({
  events: [],
  totalSeen: 0,
  append: (event) =>
    set((state) => {
      const next = state.events.concat(event);
      if (next.length > MAX_EVENTS) next.splice(0, next.length - MAX_EVENTS);
      return { events: next, totalSeen: state.totalSeen + 1 };
    }),
  clear: () => set({ events: [], totalSeen: 0 }),
}));
