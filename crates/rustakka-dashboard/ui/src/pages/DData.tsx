import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export default function DData() {
  const { data } = useQuery({ queryKey: ["ddata"], queryFn: api.ddata });
  if (!data) return <Card><CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent></Card>;

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <Card>
        <CardHeader><CardTitle>Total updates</CardTitle></CardHeader>
        <CardContent><div className="text-3xl font-semibold tabular-nums">{data.total_updates}</div></CardContent>
      </Card>
      <Card>
        <CardHeader><CardTitle>Keys <Badge variant="outline">{data.keys.length}</Badge></CardTitle></CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-1.5">
            {data.keys.length === 0 && (
              <span className="text-sm text-muted-foreground">no keys tracked</span>
            )}
            {data.keys.map((k) => (
              <Badge key={k} variant="outline" className="font-mono">{k}</Badge>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
