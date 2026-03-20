import { useTrendInterest } from "@/hooks/use-trends";
import { Card } from "@/components/ui/card";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";

interface TrendInterestChartProps {
  name: string | undefined;
}

export function TrendInterestChart({ name }: TrendInterestChartProps) {
  const { data, isLoading, error } = useTrendInterest(name);
  const chartData = data as any;

  return (
    <Card className="border border-border/60 bg-card p-5 md:p-6">
      <div className="flex items-center justify-between mb-3">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground/70 font-semibold">
            Google Search Interest · IN
          </p>
          <h2 className="font-display text-lg md:text-xl font-semibold text-foreground mt-1">
            {chartData?.name ?? name ?? "Loading trend"}
          </h2>
        </div>
        <span className="text-[11px] text-muted-foreground/80">
          Jan–Feb 2026 · indexed 0–100
        </span>
      </div>

      {isLoading ? (
        <div className="h-40 flex items-center justify-center text-xs text-muted-foreground">
          Pulling search interest…
        </div>
      ) : error || !data ? (
        <div className="h-40 flex items-center justify-center text-xs text-muted-foreground">
          Unable to load search interest.
        </div>
      ) : (
        <div className="h-48">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData?.points || []}>
              <defs>
                <linearGradient id="trendInterest" x1="0" y1="0" x2="0" y2="1">
                  <stop
                    offset="5%"
                    stopColor="hsl(var(--primary))"
                    stopOpacity={0.4}
                  />
                  <stop
                    offset="95%"
                    stopColor="hsl(var(--primary))"
                    stopOpacity={0.02}
                  />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="date"
                tickLine={false}
                axisLine={false}
                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                width={32}
                domain={[0, 100]}
                tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }}
              />
              <Tooltip
                contentStyle={{
                  borderRadius: 8,
                  border: "1px solid hsl(var(--border))",
                  backgroundColor: "hsl(var(--card))",
                  fontSize: 12,
                }}
                labelStyle={{ fontWeight: 500 }}
                formatter={(value: any) => [`${value}`, "Index"]}
              />
              <Area
                type="monotone"
                dataKey="value"
                stroke="hsl(var(--primary))"
                strokeWidth={2}
                fill="url(#trendInterest)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  );
}

