import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TBody, THead, Th, Tr, Td } from "@/components/ui/table";
import { formatNumber, formatRelative } from "@/lib/utils";

export default function Persistence() {
  const { data } = useQuery({ queryKey: ["persistence"], queryFn: api.persistence });
  if (!data) return <Card><CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent></Card>;

  return (
    <div className="grid gap-3 md:grid-cols-3">
      <Card>
        <CardHeader><CardTitle>Total events</CardTitle></CardHeader>
        <CardContent><div className="text-3xl font-semibold tabular-nums">{formatNumber(data.total_events)}</div></CardContent>
      </Card>

      <Card className="md:col-span-2">
        <CardHeader><CardTitle>Recent writes</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <THead><Tr><Th>Journal</Th><Th>Persistence id</Th><Th>Seq</Th><Th>When</Th></Tr></THead>
            <TBody>
              {data.recent_writes.slice(-50).reverse().map((w, i) => (
                <Tr key={i}>
                  <Td><Badge variant="outline">{w.journal}</Badge></Td>
                  <Td className="font-mono text-xs">{w.persistence_id}</Td>
                  <Td className="tabular-nums">{w.sequence_nr}</Td>
                  <Td className="text-xs text-muted-foreground">{formatRelative(w.timestamp)}</Td>
                </Tr>
              ))}
              {data.recent_writes.length === 0 && (
                <Tr><Td colSpan={4} className="py-4 text-center text-muted-foreground">no recent writes</Td></Tr>
              )}
            </TBody>
          </Table>
        </CardContent>
      </Card>

      {data.journals.map((j) => (
        <Card key={j.name} className="md:col-span-3">
          <CardHeader><CardTitle>{j.name}</CardTitle></CardHeader>
          <CardContent>
            <Table>
              <THead><Tr><Th>Persistence id</Th><Th>Highest seq</Th><Th>Event count</Th></Tr></THead>
              <TBody>
                {j.persistence_ids.map((p) => (
                  <Tr key={p.persistence_id}>
                    <Td className="font-mono text-xs">{p.persistence_id}</Td>
                    <Td className="tabular-nums">{p.highest_sequence_nr}</Td>
                    <Td className="tabular-nums">{p.event_count}</Td>
                  </Tr>
                ))}
              </TBody>
            </Table>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
