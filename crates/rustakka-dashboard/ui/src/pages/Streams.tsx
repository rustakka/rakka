import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TBody, THead, Th, Tr, Td } from "@/components/ui/table";
import { formatRelative } from "@/lib/utils";

export default function Streams() {
  const { data } = useQuery({ queryKey: ["streams"], queryFn: api.streams });
  if (!data) return <Card><CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent></Card>;

  return (
    <div className="grid gap-3 md:grid-cols-3">
      <Card><CardHeader><CardTitle>Running</CardTitle></CardHeader><CardContent><div className="text-3xl font-semibold tabular-nums">{data.running_graphs}</div></CardContent></Card>
      <Card><CardHeader><CardTitle>Started</CardTitle></CardHeader><CardContent><div className="text-3xl font-semibold tabular-nums">{data.total_started}</div></CardContent></Card>
      <Card><CardHeader><CardTitle>Finished</CardTitle></CardHeader><CardContent><div className="text-3xl font-semibold tabular-nums">{data.total_finished}</div></CardContent></Card>

      <Card className="md:col-span-3">
        <CardHeader><CardTitle>Active graphs <Badge variant="outline">{data.active.length}</Badge></CardTitle></CardHeader>
        <CardContent>
          <Table>
            <THead><Tr><Th>id</Th><Th>Name</Th><Th>Started</Th></Tr></THead>
            <TBody>
              {data.active.length === 0 && (
                <Tr><Td colSpan={3} className="py-4 text-center text-muted-foreground">no active graphs</Td></Tr>
              )}
              {data.active.map((g) => (
                <Tr key={g.id}>
                  <Td className="tabular-nums">{g.id}</Td>
                  <Td className="font-mono text-xs">{g.name}</Td>
                  <Td className="text-xs text-muted-foreground">{formatRelative(g.started_at)}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
