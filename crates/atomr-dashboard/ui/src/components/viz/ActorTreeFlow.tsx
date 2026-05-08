import { useMemo } from "react";
import ReactFlow, {
  Background,
  Controls,
  type Edge,
  type Node,
  MiniMap,
  Position,
} from "reactflow";
import "reactflow/dist/style.css";

import type { ActorTreeNode } from "@/lib/api";

export type Orientation = "vertical" | "horizontal";

interface Layout {
  nodes: Node[];
  edges: Edge[];
}

function layout(roots: ActorTreeNode[], orientation: Orientation): Layout {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  // Cross-axis spacing keeps siblings apart, depth-axis spacing puts
  // each generation on its own row/column. The cross-axis needs to be
  // wider in horizontal mode so labels don't overlap.
  const depthGap = orientation === "vertical" ? 100 : 260;
  const crossGap = orientation === "vertical" ? 220 : 90;

  const depthCursor: number[] = [];

  function walk(n: ActorTreeNode, depth: number, parentId?: string) {
    const id = n.path;
    depthCursor[depth] = (depthCursor[depth] ?? -1) + 1;
    const cross = depthCursor[depth] * crossGap;
    const along = depth * depthGap;
    const position =
      orientation === "vertical" ? { x: cross, y: along } : { x: along, y: cross };
    nodes.push({
      id,
      position,
      sourcePosition: orientation === "vertical" ? Position.Bottom : Position.Right,
      targetPosition: orientation === "vertical" ? Position.Top : Position.Left,
      data: {
        label: (
          <div className="flex flex-col items-start">
            <span className="font-mono text-[11px] text-muted-foreground">
              {n.name}
            </span>
            <span className="text-[10px]">{n.actor_type}</span>
            {n.mailbox_depth > 0 && (
              <span className="text-[10px] text-amber-500">
                mailbox: {n.mailbox_depth}
              </span>
            )}
          </div>
        ),
      },
      style: {
        border: "1px solid hsl(var(--border))",
        borderRadius: 8,
        padding: 8,
        background: "hsl(var(--card))",
        color: "hsl(var(--card-foreground))",
        fontSize: 12,
      },
    });
    if (parentId) {
      edges.push({ id: `${parentId}->${id}`, source: parentId, target: id });
    }
    for (const child of n.children) walk(child, depth + 1, id);
  }

  for (const r of roots) walk(r, 0);
  return { nodes, edges };
}

export function ActorTreeFlow({
  roots,
  onSelect,
  orientation = "vertical",
}: {
  roots: ActorTreeNode[];
  onSelect?: (path: string) => void;
  orientation?: Orientation;
}) {
  const { nodes, edges } = useMemo(
    () => layout(roots, orientation),
    [roots, orientation],
  );
  return (
    <div className="h-[60vh] md:h-[70vh] w-full rounded-lg border bg-card/40">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodeClick={(_, n) => onSelect?.(n.id)}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls position="bottom-right" />
        <MiniMap zoomable pannable className="hidden md:block" />
      </ReactFlow>
    </div>
  );
}
