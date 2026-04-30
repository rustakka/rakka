import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TBody, THead, Th, Tr, Td } from "@/components/ui/table";

export default function Sharding() {
  const { data } = useQuery({ queryKey: ["sharding"], queryFn: api.sharding });
  if (!data) return <Card><CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent></Card>;

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <Card>
        <CardHeader><CardTitle>Regions <Badge variant="outline">{data.regions.length}</Badge></CardTitle></CardHeader>
        <CardContent>
          <Table>
            <THead><Tr><Th>Region</Th><Th>Shards</Th></Tr></THead>
            <TBody>
              {data.regions.length === 0 && (
                <Tr><Td colSpan={2} className="py-4 text-center text-muted-foreground">no active regions</Td></Tr>
              )}
              {data.regions.map((r) => (
                <Tr key={r.region_id}>
                  <Td className="font-mono text-xs">{r.region_id}</Td>
                  <Td>
                    <div className="flex flex-wrap gap-1">
                      {r.shards.map((s) => <Badge key={s} variant="outline">{s}</Badge>)}
                    </div>
                  </Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Shard → region allocations</CardTitle></CardHeader>
        <CardContent>
          <Table>
            <THead><Tr><Th>Shard</Th><Th>Region</Th></Tr></THead>
            <TBody>
              {data.allocations.length === 0 && (
                <Tr><Td colSpan={2} className="py-4 text-center text-muted-foreground">no allocations</Td></Tr>
              )}
              {data.allocations.map(([shard, region]) => (
                <Tr key={shard}>
                  <Td className="font-mono text-xs">{shard}</Td>
                  <Td className="font-mono text-xs">{region}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
