import { useQuery } from "@tanstack/react-query";
import { api, type OverviewSnapshot } from "@/lib/api";
import { SparklineCard } from "@/components/viz/SparklineCard";
import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { useHistory } from "@/lib/history";
import { formatRelative } from "@/lib/utils";

function MiniCards({ ov }: { ov: OverviewSnapshot }) {
  const actors = useHistory(ov.actor_count);
  const dead = useHistory(ov.dead_letter_count);
  const members = useHistory(ov.cluster_member_count);
  const unreachable = useHistory(ov.cluster_unreachable_count);
  const remote = useHistory(ov.remote_association_count);
  const graphs = useHistory(ov.running_graphs);
  const events = useHistory(ov.persistence_event_count);
  const keys = useHistory(ov.ddata_key_count);

  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
      <SparklineCard title="Actors" value={ov.actor_count} history={actors} />
      <SparklineCard title="Dead letters" value={ov.dead_letter_count} history={dead} />
      <SparklineCard title="Cluster members" value={ov.cluster_member_count} history={members} />
      <SparklineCard title="Unreachable" value={ov.cluster_unreachable_count} history={unreachable} />
      <SparklineCard title="Remote assoc." value={ov.remote_association_count} history={remote} />
      <SparklineCard title="Running graphs" value={ov.running_graphs} history={graphs} />
      <SparklineCard title="Journal events" value={ov.persistence_event_count} history={events} />
      <SparklineCard title="DData keys" value={ov.ddata_key_count} history={keys} />
    </div>
  );
}

export default function Overview() {
  const { data, isLoading } = useQuery({ queryKey: ["overview"], queryFn: api.overview });

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>Overview</CardTitle>
            <p className="text-xs text-muted-foreground">
              {data?.node ? (
                <>
                  node <Badge className="ml-1">{data.node}</Badge> updated {formatRelative(data.generated_at)}
                </>
              ) : (
                "loading"
              )}
            </p>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {isLoading || !data ? (
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              {Array.from({ length: 8 }).map((_, i) => (
                <Skeleton key={i} className="h-24" />
              ))}
            </div>
          ) : (
            <MiniCards ov={data} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
