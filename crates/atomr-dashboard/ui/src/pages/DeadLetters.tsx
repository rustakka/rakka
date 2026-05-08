import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, type DeadLetterRecord } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, THead, TBody, Th, Tr, Td } from "@/components/ui/table";
import { formatRelative } from "@/lib/utils";
import { useEventsStore } from "@/store/events";
import type { TelemetryEvent } from "@/lib/ws";

function isDeadLetter(e: TelemetryEvent): e is TelemetryEvent & { kind: "dead_letter" } {
  return e.kind === "dead_letter";
}

export default function DeadLetters() {
  const [filter, setFilter] = useState("");
  const [follow, setFollow] = useState(true);

  const { data = [], isLoading } = useQuery({
    queryKey: ["dead-letters"],
    queryFn: () => api.deadLetters(200),
  });

  // Select the raw events array (stable reference until events mutate)
  // and memoize the filter/cast outside the selector. Returning a fresh
  // array from the selector on every render trips React error #185
  // (max update depth) under Zustand's default `Object.is` comparator.
  const events = useEventsStore((s) => s.events);
  const live = useMemo(
    () => events.filter(isDeadLetter).map((e) => e as unknown as DeadLetterRecord),
    [events],
  );

  const merged = useMemo(() => {
    if (!follow) return data;
    const seen = new Set<number>();
    const combined: DeadLetterRecord[] = [];
    for (const r of live.slice().reverse()) {
      if (!seen.has(r.seq)) {
        seen.add(r.seq);
        combined.push(r);
      }
    }
    for (const r of data) {
      if (!seen.has(r.seq)) {
        seen.add(r.seq);
        combined.push(r);
      }
    }
    return combined;
  }, [data, live, follow]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return merged;
    return merged.filter((r) =>
      [r.recipient, r.sender ?? "", r.message_type, r.message_preview]
        .join(" ")
        .toLowerCase()
        .includes(q),
    );
  }, [merged, filter]);

  return (
    <Card>
      <CardHeader className="gap-2">
        <div className="flex items-center justify-between">
          <CardTitle>
            Dead letters <Badge variant="outline">{filtered.length}</Badge>
          </CardTitle>
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <input
              type="checkbox"
              checked={follow}
              onChange={(e) => setFollow(e.target.checked)}
            />
            live follow
          </label>
        </div>
        <input
          type="search"
          placeholder="filter by recipient, sender, or type…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="h-9 rounded-md border bg-background px-3 text-sm"
        />
      </CardHeader>
      <CardContent>
        <Table>
          <THead>
            <Tr>
              <Th>Seq</Th>
              <Th>Recipient</Th>
              <Th>Sender</Th>
              <Th>Type</Th>
              <Th>Preview</Th>
              <Th>When</Th>
            </Tr>
          </THead>
          <TBody>
            {isLoading && (
              <Tr>
                <Td colSpan={6} className="py-4 text-center text-muted-foreground">
                  loading…
                </Td>
              </Tr>
            )}
            {!isLoading && filtered.length === 0 && (
              <Tr>
                <Td colSpan={6} className="py-6 text-center text-muted-foreground">
                  no dead letters
                </Td>
              </Tr>
            )}
            {filtered.slice(0, 500).map((r) => (
              <Tr key={r.seq}>
                <Td className="tabular-nums text-muted-foreground">{r.seq}</Td>
                <Td className="font-mono text-xs">{r.recipient}</Td>
                <Td className="font-mono text-xs">{r.sender ?? "—"}</Td>
                <Td>
                  <Badge variant="outline">{r.message_type}</Badge>
                </Td>
                <Td className="max-w-[320px] truncate text-xs text-muted-foreground">
                  {r.message_preview}
                </Td>
                <Td className="text-xs text-muted-foreground">
                  {formatRelative(r.timestamp)}
                </Td>
              </Tr>
            ))}
          </TBody>
        </Table>
      </CardContent>
    </Card>
  );
}
