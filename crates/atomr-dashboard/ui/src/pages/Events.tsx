import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TBody, THead, Th, Tr, Td } from "@/components/ui/table";
import { useEventsStore } from "@/store/events";

const TOPICS = [
  "actors",
  "dead_letters",
  "cluster",
  "sharding",
  "persistence",
  "remote",
  "streams",
  "ddata",
];

function topicOfKind(kind: string): string {
  if (kind.startsWith("actor") || kind.startsWith("mailbox")) return "actors";
  if (kind === "dead_letter") return "dead_letters";
  if (kind === "cluster_changed") return "cluster";
  if (kind === "sharding_changed") return "sharding";
  if (kind === "journal_write") return "persistence";
  if (kind === "remote_association") return "remote";
  if (kind.startsWith("streams_")) return "streams";
  if (kind === "d_data_updated") return "ddata";
  return "other";
}

export default function Events() {
  const events = useEventsStore((s) => s.events);
  const clear = useEventsStore((s) => s.clear);
  const [selected, setSelected] = useState<Record<string, boolean>>(() =>
    Object.fromEntries(TOPICS.map((t) => [t, true])),
  );

  const filtered = events
    .slice()
    .reverse()
    .filter((e) => selected[topicOfKind(e.kind as string)]);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle>Events <Badge variant="outline">{filtered.length}</Badge></CardTitle>
          <button
            type="button"
            className="rounded-md border px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
            onClick={clear}
          >
            clear
          </button>
        </div>
        <div className="flex flex-wrap gap-1.5 pt-2">
          {TOPICS.map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setSelected((s) => ({ ...s, [t]: !s[t] }))}
              className={
                "rounded-md border px-2 py-0.5 text-[11px] transition-colors " +
                (selected[t]
                  ? "bg-primary/15 text-primary border-primary/40"
                  : "text-muted-foreground")
              }
            >
              {t}
            </button>
          ))}
        </div>
      </CardHeader>
      <CardContent>
        <Table>
          <THead><Tr><Th>Kind</Th><Th>Topic</Th><Th>Payload</Th></Tr></THead>
          <TBody>
            {filtered.length === 0 && (
              <Tr><Td colSpan={3} className="py-4 text-center text-muted-foreground">no events received yet</Td></Tr>
            )}
            {filtered.slice(0, 300).map((e, i) => (
              <Tr key={`${e.kind}-${i}`}>
                <Td><Badge>{e.kind as string}</Badge></Td>
                <Td className="text-xs text-muted-foreground">{topicOfKind(e.kind as string)}</Td>
                <Td className="font-mono text-[11px]">
                  <code className="whitespace-pre-wrap break-all">{JSON.stringify(e)}</code>
                </Td>
              </Tr>
            ))}
          </TBody>
        </Table>
      </CardContent>
    </Card>
  );
}
