import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ResponsiveContainer, Line, LineChart, YAxis } from "recharts";
import { formatNumber } from "@/lib/utils";

export interface SparklineCardProps {
  title: string;
  value: number;
  history: number[];
  accent?: string;
}

export function SparklineCard({
  title,
  value,
  history,
  accent = "hsl(var(--primary))",
}: SparklineCardProps) {
  const data = history.map((v, i) => ({ i, v }));
  return (
    <Card>
      <CardHeader className="pb-0">
        <CardTitle className="text-xs uppercase tracking-wider text-muted-foreground">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="flex items-end justify-between gap-3 pt-2">
        <div className="text-2xl font-semibold">{formatNumber(value)}</div>
        <div className="h-10 flex-1 min-w-[60px]">
          <ResponsiveContainer>
            <LineChart data={data} margin={{ top: 2, bottom: 2, left: 0, right: 0 }}>
              <YAxis hide domain={["auto", "auto"]} />
              <Line
                type="monotone"
                dataKey="v"
                stroke={accent}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  );
}
