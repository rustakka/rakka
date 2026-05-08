import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type ActorTreeNode } from "@/lib/api";
import { useEventsStore } from "@/store/events";
import type { TelemetryEvent } from "@/lib/ws";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

type Hue = "info" | "warn" | "danger" | "ok" | "purple" | "pink";

interface Sim {
  path: string;
  name: string;
  type: string;
  parent?: string;
  depth: number;
  host?: string;
  x: number;
  y: number;
  vx: number;
  vy: number;
  pinned: boolean;
}

/** Axis-aligned rectangular region a node is constrained to. */
interface HostRegion {
  host: string;
  x: number;
  y: number;
  w: number;
  h: number;
  reachable: boolean;
  isSelf: boolean;
  status: string;
}

interface Particle {
  id: number;
  fromPath: string;
  toPath: string;
  startedAt: number;
  durationMs: number;
  hue: Hue;
}

interface Pulse {
  id: number;
  path: string;
  startedAt: number;
  durationMs: number;
  hue: Hue;
}

const PARTICLE_DURATION = 900;
const PULSE_DURATION = 700;

// ---------------------------------------------------------------------------
//  Force-directed simulation primitives.
//
//  We run a small custom solver (no d3 dep) that's plenty for ~30 nodes:
//    * spring force pulls children toward parents at a viewport-scaled rest length
//    * pairwise repulsion pushes everything apart (Coulomb-ish)
//    * a weak gravity field re-centers free-floating clusters
//    * velocity damping to settle the system
//  Pinned nodes (currently being dragged) ignore force integration and
//  use the cursor's position directly.
//  The rest length and repulsion constant are scaled with viewport so the
//  graph naturally fills the available canvas instead of clumping.
// ---------------------------------------------------------------------------

const SPRING_K = 0.045;
const CENTER_K = 0.0025;
const DAMPING = 0.86;
const PADDING = 60;

interface ForceParams {
  restLength: number;
  repelK: number;
}

function paramsFor(width: number, height: number, n: number): ForceParams {
  // Pick a rest length that roughly tiles the canvas with `n` nodes,
  // then dial repulsion to keep equilibrium near that length.
  const span = Math.min(width, height);
  const restLength = Math.max(80, (span / Math.sqrt(Math.max(n, 4))) * 0.85);
  // Repulsion ~ (rest-length)^2 keeps the equilibrium near restLength.
  const repelK = restLength * restLength * 0.18;
  return { restLength, repelK };
}

/**
 * Step the physics one tick. When `regions` is non-null each node is
 * additionally constrained (force-clamp + position clamp) to its
 * matching host's rectangle so the cluster boundaries become a real
 * physical wall — not just a visual annotation.
 */
function step(
  sims: Sim[],
  width: number,
  height: number,
  regions: Map<string, HostRegion> | null,
) {
  const cx = width / 2;
  const cy = height / 2;
  const { restLength, repelK } = paramsFor(width, height, sims.length);
  // Pairwise repulsion.
  for (let i = 0; i < sims.length; i++) {
    const a = sims[i];
    for (let j = i + 1; j < sims.length; j++) {
      const b = sims[j];
      let dx = a.x - b.x;
      let dy = a.y - b.y;
      let d2 = dx * dx + dy * dy;
      if (d2 < 1) {
        dx += (Math.random() - 0.5) * 4;
        dy += (Math.random() - 0.5) * 4;
        d2 = dx * dx + dy * dy + 1;
      }
      const f = repelK / d2;
      const d = Math.sqrt(d2);
      const fx = (dx / d) * f;
      const fy = (dy / d) * f;
      a.vx += fx;
      a.vy += fy;
      b.vx -= fx;
      b.vy -= fy;
    }
  }
  // Spring along parent edges.
  const byPath = new Map<string, Sim>();
  for (const n of sims) byPath.set(n.path, n);
  for (const n of sims) {
    if (!n.parent) continue;
    const p = byPath.get(n.parent);
    if (!p) continue;
    const dx = p.x - n.x;
    const dy = p.y - n.y;
    const d = Math.sqrt(dx * dx + dy * dy) || 1;
    const f = SPRING_K * (d - restLength);
    const fx = (dx / d) * f;
    const fy = (dy / d) * f;
    n.vx += fx;
    n.vy += fy;
    p.vx -= fx * 0.5;
    p.vy -= fy * 0.5;
  }
  // Gentle gravity. Without per-host regions, pull toward canvas centre.
  // With regions on, pull toward the host's centre instead so each
  // group settles inside its own box.
  for (const n of sims) {
    if (regions) {
      const r = n.host ? regions.get(n.host) : undefined;
      if (r) {
        const tx = r.x + r.w / 2;
        const ty = r.y + r.h / 2;
        n.vx += (tx - n.x) * (CENTER_K * 4);
        n.vy += (ty - n.y) * (CENTER_K * 4);
      } else {
        n.vx += (cx - n.x) * CENTER_K;
        n.vy += (cy - n.y) * CENTER_K;
      }
    } else {
      n.vx += (cx - n.x) * CENTER_K;
      n.vy += (cy - n.y) * CENTER_K;
    }
  }
  // Integrate + clamp.
  for (const n of sims) {
    if (n.pinned) {
      n.vx = 0;
      n.vy = 0;
      // Pinned (dragged) nodes are clamped into their host region by
      // the drag handler itself; nothing to do here.
      continue;
    }
    n.vx *= DAMPING;
    n.vy *= DAMPING;
    n.x += n.vx;
    n.y += n.vy;
    // Per-host walls when host visualization is on; canvas walls
    // otherwise.
    let minX = PADDING;
    let minY = PADDING;
    let maxX = width - PADDING;
    let maxY = height - PADDING;
    if (regions) {
      const r = n.host ? regions.get(n.host) : undefined;
      if (r) {
        const inset = 20;
        minX = r.x + inset;
        minY = r.y + inset;
        maxX = r.x + r.w - inset;
        maxY = r.y + r.h - inset;
      }
    }
    if (n.x < minX) {
      n.x = minX;
      if (n.vx < 0) n.vx = 0;
    }
    if (n.y < minY) {
      n.y = minY;
      if (n.vy < 0) n.vy = 0;
    }
    if (n.x > maxX) {
      n.x = maxX;
      if (n.vx > 0) n.vx = 0;
    }
    if (n.y > maxY) {
      n.y = maxY;
      if (n.vy > 0) n.vy = 0;
    }
  }
}

// Walk the tree to get path + parent + depth + host for every actor.
interface Walked {
  path: string;
  name: string;
  type: string;
  parent?: string;
  depth: number;
  host?: string;
}
function walk(roots: ActorTreeNode[]): Walked[] {
  const out: Walked[] = [];
  function go(n: ActorTreeNode, parent: string | undefined, depth: number) {
    out.push({ path: n.path, name: n.name, type: n.actor_type, parent, depth, host: n.host });
    for (const c of n.children) go(c, n.path, depth + 1);
  }
  for (const r of roots) go(r, undefined, 0);
  return out;
}

/**
 * Allocate a rectangular region of the canvas to each cluster host. The
 * grid is laid out in row-major order (sized to roughly square the
 * member count) with a small gutter between cells.
 */
function buildHostRegions(
  members: { address: string; status: string; reachable: boolean }[],
  selfAddr: string | null,
  width: number,
  height: number,
): Map<string, HostRegion> {
  const out = new Map<string, HostRegion>();
  if (members.length === 0) return out;
  const cols = Math.ceil(Math.sqrt(members.length));
  const rows = Math.ceil(members.length / cols);
  const gutter = 14;
  const headerSpace = 20; // room for the host label inside each cell
  const cellW = (width - gutter * (cols + 1)) / cols;
  const cellH = (height - gutter * (rows + 1)) / rows;
  members.forEach((m, i) => {
    const c = i % cols;
    const r = Math.floor(i / cols);
    out.set(m.address, {
      host: m.address,
      x: gutter + c * (cellW + gutter),
      y: gutter + r * (cellH + gutter) + headerSpace * 0.0,
      w: cellW,
      h: cellH,
      reachable: m.reachable,
      isSelf: m.address === selfAddr,
      status: m.status,
    });
  });
  return out;
}

function useViewportSize<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [size, setSize] = useState({ width: 800, height: 600 });
  useEffect(() => {
    if (!ref.current) return;
    const el = ref.current;
    const ro = new ResizeObserver(() => {
      setSize({ width: el.clientWidth, height: el.clientHeight });
    });
    ro.observe(el);
    setSize({ width: el.clientWidth, height: el.clientHeight });
    return () => ro.disconnect();
  }, []);
  return [ref, size] as const;
}

// ---------------------------------------------------------------------------
//  Page
// ---------------------------------------------------------------------------

export default function Topology() {
  const { data } = useQuery({ queryKey: ["actors"], queryFn: api.actors, refetchInterval: 2000 });
  const { data: cluster } = useQuery({
    queryKey: ["cluster-state"],
    queryFn: api.clusterState,
    refetchInterval: 3000,
  });
  const events = useEventsStore((s) => s.events);
  const totalSeen = useEventsStore((s) => s.totalSeen);
  const [animate, setAnimate] = useState(true);
  const [showHosts, setShowHosts] = useState(false);

  const [containerRef, size] = useViewportSize<HTMLDivElement>();

  // Persist sims across renders so the simulation has continuity. We
  // reconcile by `path`: existing sims keep their position/velocity,
  // new actors seeded near their parent, removed actors dropped.
  const simsRef = useRef<Map<string, Sim>>(new Map());
  const [, setRenderTick] = useState(0);

  useEffect(() => {
    const flat = data ? walk(data.roots) : [];
    const next = new Map<string, Sim>();
    for (const w of flat) {
      const existing = simsRef.current.get(w.path);
      if (existing) {
        existing.name = w.name;
        existing.type = w.type;
        existing.parent = w.parent;
        existing.depth = w.depth;
        existing.host = w.host;
        next.set(w.path, existing);
      } else {
        const parent = w.parent ? simsRef.current.get(w.parent) : undefined;
        const px = parent ? parent.x : size.width / 2;
        const py = parent ? parent.y : size.height / 2;
        const ang = Math.random() * Math.PI * 2;
        next.set(w.path, {
          path: w.path,
          name: w.name,
          type: w.type,
          parent: w.parent,
          depth: w.depth,
          host: w.host,
          x: px + Math.cos(ang) * 40,
          y: py + Math.sin(ang) * 40,
          vx: 0,
          vy: 0,
          pinned: false,
        });
      }
    }
    simsRef.current = next;
  }, [data, size.width, size.height]);

  // Animation/event loop.
  const particlesRef = useRef<Particle[]>([]);
  const pulsesRef = useRef<Pulse[]>([]);
  /// Monotonic count of events processed by this page so far. Compared
  /// against the store's `totalSeen` to derive how many fresh events
  /// arrived since the last render. Using `events.length` here would
  /// silently stop after MAX_EVENTS because the ring buffer is capped.
  const lastSeenRef = useRef(0);
  const seqRef = useRef(0);

  useEffect(() => {
    if (!animate) {
      // Visualization paused — stay caught up so resuming doesn't
      // dump a flood of stale events.
      lastSeenRef.current = totalSeen;
      return;
    }
    const arrived = totalSeen - lastSeenRef.current;
    if (arrived <= 0) return;
    // The events array is bounded; if more events arrived than the
    // buffer holds we've already dropped some — process the visible tail.
    const take = Math.min(arrived, events.length);
    const fresh = events.slice(events.length - take);
    lastSeenRef.current = totalSeen;
    const now = Date.now();
    const sims = simsRef.current;

    // Pre-compute helpers — pick a worker path matching `worker-N` for
    // events that don't carry a precise actor path (e.g. journal_write
    // says `worker:worker-3`, we resolve to `…/router/worker-3`).
    const workerPaths = new Map<string, string>();
    sims.forEach((s) => {
      if (s.path.endsWith(`/${s.name}`) && s.name.startsWith("worker-")) {
        workerPaths.set(s.name, s.path);
      }
    });
    const persisterPath = [...sims.values()].find((s) => s.name === "persister")?.path;
    const aggregatorPath = [...sims.values()].find((s) => s.name === "aggregator")?.path;
    const rootPath = [...sims.values()].find((s) => !s.parent)?.path;

    for (const e of fresh) {
      switch (e.kind) {
        case "actor_spawned": {
          const evt = e as TelemetryEvent & { path: string; parent: string | null };
          if (evt.parent && sims.has(evt.parent) && sims.has(evt.path)) {
            seqRef.current += 1;
            particlesRef.current.push({
              id: seqRef.current,
              fromPath: evt.parent,
              toPath: evt.path,
              startedAt: now,
              durationMs: PARTICLE_DURATION,
              hue: "ok",
            });
          }
          if (sims.has(evt.path)) {
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: evt.path,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "ok",
            });
          }
          break;
        }
        case "actor_stopped": {
          const evt = e as TelemetryEvent & { path: string };
          if (sims.has(evt.path)) {
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: evt.path,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "warn",
            });
          }
          break;
        }
        case "mailbox_sampled": {
          const evt = e as TelemetryEvent & { path: string; depth: number };
          const n = sims.get(evt.path);
          if (!n) break;
          seqRef.current += 1;
          pulsesRef.current.push({
            id: seqRef.current,
            path: evt.path,
            startedAt: now,
            durationMs: PULSE_DURATION,
            hue: "info",
          });
          if (n.parent && sims.has(n.parent)) {
            seqRef.current += 1;
            particlesRef.current.push({
              id: seqRef.current,
              fromPath: n.parent,
              toPath: evt.path,
              startedAt: now,
              durationMs: PARTICLE_DURATION,
              hue: "info",
            });
          }
          break;
        }
        case "dead_letter": {
          const evt = e as TelemetryEvent & { recipient: string; sender: string | null };
          if (sims.has(evt.recipient)) {
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: evt.recipient,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "danger",
            });
          }
          if (evt.sender && sims.has(evt.sender) && sims.has(evt.recipient)) {
            seqRef.current += 1;
            particlesRef.current.push({
              id: seqRef.current,
              fromPath: evt.sender,
              toPath: evt.recipient,
              startedAt: now,
              durationMs: PARTICLE_DURATION,
              hue: "danger",
            });
          }
          break;
        }
        case "journal_write": {
          // `persistence_id` is `worker:worker-N`; resolve to that worker's
          // path and animate worker → persister.
          const evt = e as TelemetryEvent & { persistence_id: string };
          const workerName = evt.persistence_id.split(":").pop() ?? "";
          const fromPath = workerPaths.get(workerName);
          if (fromPath && persisterPath) {
            seqRef.current += 1;
            particlesRef.current.push({
              id: seqRef.current,
              fromPath,
              toPath: persisterPath,
              startedAt: now,
              durationMs: PARTICLE_DURATION,
              hue: "purple",
            });
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: persisterPath,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "purple",
            });
          }
          break;
        }
        case "streams_graph_started":
        case "streams_graph_finished": {
          // Streams aren't tied to a specific actor; flash the root so
          // there's a visible signal.
          if (rootPath) {
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: rootPath,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "pink",
            });
          }
          break;
        }
        case "d_data_updated":
        case "cluster_changed":
        case "sharding_changed":
        case "remote_association": {
          // External-system events: brief pink pulse on the aggregator
          // (where data converges), so the page shows that *something*
          // is happening even when the actor traffic is quiet.
          if (aggregatorPath) {
            seqRef.current += 1;
            pulsesRef.current.push({
              id: seqRef.current,
              path: aggregatorPath,
              startedAt: now,
              durationMs: PULSE_DURATION,
              hue: "pink",
            });
          }
          break;
        }
      }
    }
    // Cap memory.
    if (particlesRef.current.length > 400) {
      particlesRef.current.splice(0, particlesRef.current.length - 400);
    }
    if (pulsesRef.current.length > 400) {
      pulsesRef.current.splice(0, pulsesRef.current.length - 400);
    }
  }, [totalSeen, events, animate]);

  // The render loop: step the physics + filter expired effects + force a
  // re-render. ~30 fps via requestAnimationFrame.
  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      if (cancelled) return;
      const sims = [...simsRef.current.values()];
      step(sims, size.width, size.height, regionsRef.current);
      const t = Date.now();
      particlesRef.current = particlesRef.current.filter((p) => t - p.startedAt < p.durationMs);
      pulsesRef.current = pulsesRef.current.filter((p) => t - p.startedAt < p.durationMs);
      setRenderTick((n) => (n + 1) & 0xffff);
      requestAnimationFrame(tick);
    };
    const handle = requestAnimationFrame(tick);
    return () => {
      cancelled = true;
      cancelAnimationFrame(handle);
    };
  }, [size.width, size.height]);

  // ----- Drag handling -----
  const draggingRef = useRef<string | null>(null);
  const onPointerDown = (path: string) => (ev: React.PointerEvent<SVGElement>) => {
    ev.stopPropagation();
    draggingRef.current = path;
    const sim = simsRef.current.get(path);
    if (sim) sim.pinned = true;
    (ev.target as Element).setPointerCapture?.(ev.pointerId);
  };
  const onPointerMove = (ev: React.PointerEvent<SVGSVGElement>) => {
    const path = draggingRef.current;
    if (!path) return;
    const svg = ev.currentTarget;
    const rect = svg.getBoundingClientRect();
    const sim = simsRef.current.get(path);
    if (!sim) return;
    let nx = ev.clientX - rect.left;
    let ny = ev.clientY - rect.top;
    // Clamp drag into the host's region when host visualization is on,
    // so dragging visually reinforces "this actor lives on this host."
    const regions = regionsRef.current;
    if (regions) {
      const r = sim.host ? regions.get(sim.host) : undefined;
      if (r) {
        const inset = 20;
        const minX = r.x + inset;
        const minY = r.y + inset;
        const maxX = r.x + r.w - inset;
        const maxY = r.y + r.h - inset;
        if (nx < minX) nx = minX;
        if (ny < minY) ny = minY;
        if (nx > maxX) nx = maxX;
        if (ny > maxY) ny = maxY;
      }
    }
    sim.x = nx;
    sim.y = ny;
    sim.vx = 0;
    sim.vy = 0;
  };
  const onPointerUp = () => {
    const path = draggingRef.current;
    if (!path) return;
    const sim = simsRef.current.get(path);
    if (sim) sim.pinned = false;
    draggingRef.current = null;
  };

  const sims = useMemo(() => [...simsRef.current.values()], [simsRef.current.size, size]);
  void sims; // referenced for re-render trigger only

  const renderSims = [...simsRef.current.values()];
  const edges: { a: Sim; b: Sim }[] = [];
  for (const n of renderSims) {
    if (!n.parent) continue;
    const p = simsRef.current.get(n.parent);
    if (p) edges.push({ a: p, b: n });
  }

  // ---- Host boundary geometry ----------------------------------------------
  // When the toggle is on, every cluster member gets its own rectangular
  // region of the canvas. Actors are pinned to their host's region by
  // both the force solver and the drag handler, so dragging visually
  // reinforces the cluster topology instead of letting actors hop hosts.
  const hostRegions = useMemo<Map<string, HostRegion> | null>(() => {
    if (!showHosts || !cluster || size.width <= 0 || size.height <= 0) return null;
    return buildHostRegions(
      (cluster.members ?? []).map((m) => ({
        address: m.address,
        status: m.status,
        reachable: m.reachable,
      })),
      cluster.self_address,
      size.width,
      size.height,
    );
  }, [showHosts, cluster, size.width, size.height]);
  const regionsRef = useRef<Map<string, HostRegion> | null>(null);
  regionsRef.current = hostRegions;

  return (
    <Card className="overflow-hidden">
      <CardHeader className="flex flex-row items-center justify-between gap-2">
        <CardTitle className="flex items-center gap-2">
          Topology <Badge variant="outline">{renderSims.length} actors</Badge>
        </CardTitle>
        <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
          <button
            type="button"
            onClick={() => {
              if (animate) {
                // Clear visible effects so the canvas goes quiet immediately.
                particlesRef.current = [];
                pulsesRef.current = [];
              }
              setAnimate((v) => !v);
            }}
            aria-pressed={!animate}
            className={
              "rounded-md border px-2 py-1 text-xs " +
              (animate
                ? "bg-emerald-500/15 text-emerald-400 hover:bg-emerald-500/25"
                : "bg-muted text-muted-foreground hover:bg-muted/70")
            }
            title={animate ? "Pause activity visualization" : "Resume activity visualization"}
          >
            {animate ? "● activity on" : "○ activity off"}
          </button>
          <button
            type="button"
            onClick={() => setShowHosts((v) => !v)}
            aria-pressed={showHosts}
            className={
              "rounded-md border px-2 py-1 text-xs " +
              (showHosts
                ? "bg-sky-500/15 text-sky-400 hover:bg-sky-500/25"
                : "bg-muted text-muted-foreground hover:bg-muted/70")
            }
            title={
              showHosts
                ? "Hide cluster host boundaries"
                : "Show dotted boundaries around each cluster host"
            }
          >
            {showHosts ? "▣ hosts on" : "▢ hosts off"}
          </button>
          <Legend label="spawn" hue="ok" />
          <Legend label="message" hue="info" />
          <Legend label="stop" hue="warn" />
          <Legend label="dead letter" hue="danger" />
          <Legend label="journal write" hue="purple" />
          <Legend label="cluster / ddata / streams" hue="pink" />
          <span className="text-[10px] italic">drag any node to rearrange</span>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div ref={containerRef} className="relative h-[70vh] w-full overflow-hidden bg-card/40">
          <svg
            width={size.width}
            height={size.height}
            className="absolute inset-0 select-none"
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
            onPointerLeave={onPointerUp}
          >
            {/* Host boundaries (rendered behind everything else) */}
            {hostRegions &&
              [...hostRegions.values()].map((r) => (
                <g
                  key={r.host}
                  className={r.isSelf ? "text-sky-400" : "text-muted-foreground"}
                  opacity={r.reachable ? 1 : 0.45}
                >
                  <rect
                    x={r.x}
                    y={r.y}
                    width={r.w}
                    height={r.h}
                    rx={14}
                    ry={14}
                    fill={r.isSelf ? "hsl(var(--sky) / 0.04)" : "hsl(var(--muted) / 0.15)"}
                    stroke="currentColor"
                    strokeOpacity={r.isSelf ? 0.6 : 0.4}
                    strokeWidth={1.3}
                    strokeDasharray={r.isSelf ? "6 5" : "4 5"}
                  />
                  <text
                    x={r.x + 12}
                    y={r.y + 18}
                    fontSize={11}
                    fill="currentColor"
                    className="font-mono"
                  >
                    ⌂ {r.host}{r.isSelf ? "  · self" : ""}
                  </text>
                  <text
                    x={r.x + r.w - 12}
                    y={r.y + 18}
                    textAnchor="end"
                    fontSize={10}
                    fill="currentColor"
                    opacity={0.7}
                  >
                    {r.status}{!r.reachable ? " · unreachable" : ""}
                  </text>
                </g>
              ))}
            {edges.map((e) => (
              <line
                key={`${e.a.path}->${e.b.path}`}
                x1={e.a.x}
                y1={e.a.y}
                x2={e.b.x}
                y2={e.b.y}
                stroke="hsl(var(--muted-foreground) / 0.25)"
                strokeWidth={1}
              />
            ))}
            {particlesRef.current.map((p) => {
              const a = simsRef.current.get(p.fromPath);
              const b = simsRef.current.get(p.toPath);
              if (!a || !b) return null;
              const t = Math.min(1, (Date.now() - p.startedAt) / p.durationMs);
              const x = a.x + (b.x - a.x) * t;
              const y = a.y + (b.y - a.y) * t;
              return (
                <circle
                  key={p.id}
                  cx={x}
                  cy={y}
                  r={4}
                  className={hueFill(p.hue)}
                  opacity={1 - t * 0.4}
                />
              );
            })}
            {pulsesRef.current.map((pulse) => {
              const n = simsRef.current.get(pulse.path);
              if (!n) return null;
              const elapsed = Date.now() - pulse.startedAt;
              const t = Math.min(1, elapsed / pulse.durationMs);
              const r = 12 + t * 22;
              const opacity = (1 - t) * 0.7;
              return (
                <circle
                  key={pulse.id}
                  cx={n.x}
                  cy={n.y}
                  r={r}
                  fill="none"
                  className={hueStroke(pulse.hue)}
                  opacity={opacity}
                  strokeWidth={2}
                />
              );
            })}
            {renderSims.map((n) => (
              <g
                key={n.path}
                onPointerDown={onPointerDown(n.path)}
                style={{ cursor: "grab" }}
              >
                <circle
                  cx={n.x}
                  cy={n.y}
                  r={n.depth === 0 ? 16 : 10}
                  fill={n.pinned ? "hsl(var(--accent))" : "hsl(var(--card))"}
                  stroke="hsl(var(--foreground))"
                  strokeWidth={n.depth === 0 ? 2 : 1}
                />
                <text
                  x={n.x}
                  y={n.y + (n.depth === 0 ? 32 : 24)}
                  textAnchor="middle"
                  fontSize={11}
                  fill="hsl(var(--foreground))"
                  className="pointer-events-none select-none font-mono"
                >
                  {n.name}
                </text>
              </g>
            ))}
          </svg>
          {renderSims.length === 0 && (
            <div className="absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">
              waiting for the actor system to come up…
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function hueFill(hue: Hue): string {
  switch (hue) {
    case "ok":
      return "fill-emerald-400";
    case "info":
      return "fill-sky-400";
    case "warn":
      return "fill-amber-400";
    case "danger":
      return "fill-rose-400";
    case "purple":
      return "fill-violet-400";
    case "pink":
      return "fill-fuchsia-400";
  }
}

function hueStroke(hue: Hue): string {
  switch (hue) {
    case "ok":
      return "stroke-emerald-400";
    case "info":
      return "stroke-sky-400";
    case "warn":
      return "stroke-amber-400";
    case "danger":
      return "stroke-rose-400";
    case "purple":
      return "stroke-violet-400";
    case "pink":
      return "stroke-fuchsia-400";
  }
}

function Legend({ label, hue }: { label: string; hue: Hue }) {
  return (
    <span className="flex items-center gap-1">
      <svg width={10} height={10}>
        <circle cx={5} cy={5} r={4} className={hueFill(hue)} />
      </svg>
      {label}
    </span>
  );
}
