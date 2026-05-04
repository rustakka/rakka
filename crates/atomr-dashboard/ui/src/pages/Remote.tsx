import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TBody, THead, Th, Tr, Td } from "@/components/ui/table";
import { formatNumber } from "@/lib/utils";

export default function Remote() {
  const { data } = useQuery({ queryKey: ["remote"], queryFn: api.remote });
  if (!data) return <Card><CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent></Card>;

  return (
    <Card>
      <CardHeader><CardTitle>Remote associations <Badge variant="outline">{data.associations.length}</Badge></CardTitle></CardHeader>
      <CardContent>
        <Table>
          <THead>
            <Tr>
              <Th>Remote address</Th>
              <Th>State</Th>
              <Th>Inbound bytes</Th>
              <Th>Outbound bytes</Th>
            </Tr>
          </THead>
          <TBody>
            {data.associations.length === 0 && (
              <Tr><Td colSpan={4} className="py-4 text-center text-muted-foreground">no associations</Td></Tr>
            )}
            {data.associations.map((a) => (
              <Tr key={a.remote_address}>
                <Td className="font-mono text-xs">{a.remote_address}</Td>
                <Td>
                  <Badge variant={a.state === "active" ? "success" : "outline"}>{a.state}</Badge>
                </Td>
                <Td className="tabular-nums">{formatNumber(a.inbound_bytes)}</Td>
                <Td className="tabular-nums">{formatNumber(a.outbound_bytes)}</Td>
              </Tr>
            ))}
          </TBody>
        </Table>
      </CardContent>
    </Card>
  );
}
