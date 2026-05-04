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

interface Layout {
  nodes: Node[];
  edges: Edge[];
}

function layout(roots: ActorTreeNode[]): Layout {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const hGap = 220;
  const vGap = 80;

  let depthCursor: number[] = [];

  function walk(n: ActorTreeNode, depth: number, parentId?: string) {
    const id = n.path;
    depthCursor[depth] = (depthCursor[depth] ?? -1) + 1;
    const y = depth * vGap;
    const x = depthCursor[depth] * hGap;
    nodes.push({
      id,
      position: { x, y },
      sourcePosition: Position.Bottom,
      targetPosition: Position.Top,
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
}: {
  roots: ActorTreeNode[];
  onSelect?: (path: string) => void;
}) {
  const { nodes, edges } = useMemo(() => layout(roots), [roots]);
  return (
    <div className="h-[60vh] md:h-[70vh] w-full rounded-lg border bg-card/40">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodeClick={(_, n) => onSelect?.(n.id)}
        fitView
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls position="bottom-right" />
        <MiniMap zoomable pannable className="hidden md:block" />
      </ReactFlow>
    </div>
  );
}
