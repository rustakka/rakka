import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { ActorTreeFlow } from "@/components/viz/ActorTreeFlow";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export default function Actors() {
  const [selected, setSelected] = useState<string | null>(null);
  const { data, isLoading } = useQuery({ queryKey: ["actors"], queryFn: api.actors });

  const sel = useMemo(
    () => data?.flat.find((s) => s.path === selected) ?? null,
    [data, selected],
  );

  return (
    <div className="grid gap-3 md:grid-cols-[1fr_320px]">
      <Card className="overflow-hidden">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>
            Actor hierarchy{" "}
            {data && <Badge variant="outline">{data.total} live</Badge>}
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading || !data ? (
            <div className="flex h-[60vh] items-center justify-center text-sm text-muted-foreground">
              loading…
            </div>
          ) : (
            <ActorTreeFlow roots={data.roots} onSelect={setSelected} />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>{sel ? "Inspector" : "Select an actor"}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {sel ? (
            <>
              <div>
                <div className="text-xs text-muted-foreground">Path</div>
                <div className="font-mono text-xs break-all">{sel.path}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Parent</div>
                <div className="font-mono text-xs break-all">{sel.parent ?? "—"}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Type</div>
                <Badge>{sel.actor_type}</Badge>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Mailbox depth</div>
                <div className="text-lg font-semibold tabular-nums">{sel.mailbox_depth}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Spawned at</div>
                <div className="text-xs">{sel.spawned_at}</div>
              </div>
            </>
          ) : (
            <p className="text-muted-foreground">
              Click a node in the graph to see its path, parent, mailbox depth,
              and spawn time.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
