import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ClusterRing } from "@/components/viz/ClusterRing";
import { ReachabilityHeatmap } from "@/components/viz/ReachabilityHeatmap";
import { Table, TBody, THead, Th, Td, Tr } from "@/components/ui/table";

export default function Cluster() {
  const { data, isLoading } = useQuery({
    queryKey: ["cluster"],
    queryFn: api.clusterState,
  });

  if (isLoading || !data) {
    return (
      <Card>
        <CardContent className="py-6 text-sm text-muted-foreground">loading…</CardContent>
      </Card>
    );
  }

  return (
    <div className="grid gap-3 md:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>
            Topology{" "}
            <Badge variant="outline">{data.members.length} members</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ClusterRing members={data.members} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Gossip version</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <THead>
              <Tr>
                <Th>Address</Th>
                <Th>Clock</Th>
              </Tr>
            </THead>
            <TBody>
              {data.gossip_version.length === 0 && (
                <Tr>
                  <Td colSpan={2} className="py-4 text-center text-muted-foreground">
                    no gossip version data
                  </Td>
                </Tr>
              )}
              {data.gossip_version.map(([addr, v]) => (
                <Tr key={addr}>
                  <Td className="font-mono text-xs">{addr}</Td>
                  <Td className="tabular-nums">{v}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </CardContent>
      </Card>

      <Card className="md:col-span-2">
        <CardHeader>
          <CardTitle>Members</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <THead>
              <Tr>
                <Th>Address</Th>
                <Th>Status</Th>
                <Th>Roles</Th>
                <Th>Reachable</Th>
                <Th>Up #</Th>
              </Tr>
            </THead>
            <TBody>
              {data.members.map((m) => (
                <Tr key={m.address}>
                  <Td className="font-mono text-xs">{m.address}</Td>
                  <Td>
                    <Badge
                      variant={
                        m.status === "Up"
                          ? "success"
                          : m.status === "Down"
                            ? "destructive"
                            : "outline"
                      }
                    >
                      {m.status}
                    </Badge>
                  </Td>
                  <Td>
                    <div className="flex flex-wrap gap-1">
                      {m.roles.map((r) => (
                        <Badge key={r} variant="outline">{r}</Badge>
                      ))}
                    </div>
                  </Td>
                  <Td>
                    <Badge variant={m.reachable ? "success" : "destructive"}>
                      {m.reachable ? "yes" : "no"}
                    </Badge>
                  </Td>
                  <Td className="tabular-nums">{m.up_number}</Td>
                </Tr>
              ))}
            </TBody>
          </Table>
        </CardContent>
      </Card>

      <Card className="md:col-span-2">
        <CardHeader>
          <CardTitle>Reachability</CardTitle>
        </CardHeader>
        <CardContent>
          <ReachabilityHeatmap records={data.reachability_records} />
        </CardContent>
      </Card>
    </div>
  );
}
