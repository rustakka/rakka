// Connection settings — which backend the dashboard is pointed at, and
// what mode it's in. Mostly cosmetic today; `fetch` talks to relative
// paths that either hit the Vite dev proxy or the embedded service.

import { create } from "zustand";

interface ConnectionState {
  node: string;
  mode: "local" | "remote" | "cluster";
  setNode: (node: string) => void;
  setMode: (mode: "local" | "remote" | "cluster") => void;
}

export const useConnectionStore = create<ConnectionState>((set) => ({
  node: "local",
  mode: "local",
  setNode: (node) => set({ node }),
  setMode: (mode) => set({ mode }),
}));
