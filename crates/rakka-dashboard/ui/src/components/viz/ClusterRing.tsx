import type { ClusterMemberInfo } from "@/lib/api";

export function ClusterRing({ members }: { members: ClusterMemberInfo[] }) {
  const size = 260;
  const r = 100;
  const cx = size / 2;
  const cy = size / 2;
  return (
    <svg viewBox={`0 0 ${size} ${size}`} className="h-64 w-full">
      <circle cx={cx} cy={cy} r={r} fill="none" stroke="hsl(var(--border))" strokeDasharray="4 6" />
      {members.map((m, i) => {
        const angle = (i / Math.max(1, members.length)) * Math.PI * 2 - Math.PI / 2;
        const x = cx + r * Math.cos(angle);
        const y = cy + r * Math.sin(angle);
        const color = !m.reachable
          ? "hsl(var(--destructive))"
          : m.status === "Up"
            ? "hsl(var(--primary))"
            : "hsl(var(--muted-foreground))";
        return (
          <g key={m.address}>
            <circle cx={x} cy={y} r={10} fill={color} />
            <text
              x={x}
              y={y + 22}
              fontSize={9}
              textAnchor="middle"
              fill="hsl(var(--muted-foreground))"
            >
              {m.address.replace(/^akka:\/\//, "").slice(0, 18)}
            </text>
          </g>
        );
      })}
      <text
        x={cx}
        y={cy + 4}
        textAnchor="middle"
        fontSize={14}
        fill="hsl(var(--foreground))"
        fontWeight={600}
      >
        {members.length} nodes
      </text>
    </svg>
  );
}
