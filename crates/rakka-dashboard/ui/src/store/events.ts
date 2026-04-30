// Zustand store for the live telemetry event stream. Keeps a bounded
// ring buffer of the most recent events so pages can do sparklines
// without going back to the server.

import { create } from "zustand";
import type { TelemetryEvent } from "@/lib/ws";

const MAX_EVENTS = 500;

interface EventsState {
  events: TelemetryEvent[];
  append: (event: TelemetryEvent) => void;
  clear: () => void;
}

export const useEventsStore = create<EventsState>((set) => ({
  events: [],
  append: (event) =>
    set((state) => {
      const next = state.events.concat(event);
      if (next.length > MAX_EVENTS) next.splice(0, next.length - MAX_EVENTS);
      return { events: next };
    }),
  clear: () => set({ events: [] }),
}));
