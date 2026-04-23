import type { ReachabilityRecord } from "@/lib/api";
import { cn } from "@/lib/utils";

export function ReachabilityHeatmap({
  records,
}: {
  records: ReachabilityRecord[];
}) {
  const nodes = Array.from(
    new Set(records.flatMap((r) => [r.observer, r.subject])),
  ).sort();

  if (nodes.length === 0) {
    return (
      <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
        no reachability data
      </div>
    );
  }

  const lookup = new Map(
    records.map((r) => [`${r.observer}|${r.subject}`, r.status] as const),
  );

  return (
    <div className="overflow-auto">
      <table className="text-[11px]">
        <thead>
          <tr>
            <th className="sticky left-0 bg-card p-1 text-left text-muted-foreground">
              observer \ subject
            </th>
            {nodes.map((n) => (
              <th key={n} className="p-1 text-left text-muted-foreground">
                {n.replace(/^akka:\/\//, "").slice(0, 14)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {nodes.map((obs) => (
            <tr key={obs}>
              <td className="sticky left-0 bg-card p-1 font-mono text-muted-foreground">
                {obs.replace(/^akka:\/\//, "").slice(0, 14)}
              </td>
              {nodes.map((sub) => {
                const status = lookup.get(`${obs}|${sub}`) ?? (obs === sub ? "self" : "—");
                const tone =
                  status === "reachable"
                    ? "bg-emerald-500/40"
                    : status === "unreachable"
                      ? "bg-destructive/60"
                      : status === "terminated"
                        ? "bg-muted"
                        : "bg-transparent";
                return (
                  <td
                    key={sub}
                    className={cn("h-6 w-6 border border-border/40 text-center", tone)}
                    title={`${obs} → ${sub}: ${status}`}
                  />
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
