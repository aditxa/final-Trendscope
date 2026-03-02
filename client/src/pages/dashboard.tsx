import { useState, useEffect } from "react";
import { useTrends } from "@/hooks/use-trends";
import { TrendCard } from "@/components/trend-card";
import { TrendInterestChart } from "@/components/trend-interest-chart";
import { Skeleton } from "@/components/ui/skeleton";
import { UtensilsCrossed, AlertCircle } from "lucide-react";

export default function Dashboard() {
  const { data: trends, isLoading, error } = useTrends();
  const [selectedTrendName, setSelectedTrendName] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!selectedTrendName && trends && Array.isArray(trends) && trends.length > 0) {
      setSelectedTrendName(trends[0]?.name);
    }
  }, [selectedTrendName, trends]);

  return (
    <div className="min-h-screen bg-background/50 p-6 md:p-8 lg:p-10">
      <div className="max-w-6xl mx-auto space-y-8">
        
        {/* Header */}
        <div className="space-y-2">
          <h1 className="font-display text-3xl md:text-4xl font-bold tracking-tight text-foreground">
            Market Intelligence
          </h1>
          <p className="text-muted-foreground text-lg max-w-2xl">
            Real-time culinary trends detected from high-velocity social forums, validated against global search data.
          </p>
        </div>

        {/* Google Trends Overview */}
        {!isLoading && !error && trends && Array.isArray(trends) && trends.length > 0 && (
          <TrendInterestChart name={selectedTrendName ?? trends[0]?.name} />
        )}

        {/* Content Area */}
        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <div key={i} className="rounded-xl border border-border bg-card p-6 h-[320px] flex flex-col">
                <Skeleton className="h-8 w-3/4 mb-4" />
                <Skeleton className="h-4 w-1/4 mb-6" />
                <Skeleton className="h-4 w-full mb-2" />
                <Skeleton className="h-4 w-full mb-2" />
                <Skeleton className="h-4 w-2/3 mb-8" />
                <div className="mt-auto grid grid-cols-2 gap-4">
                  <Skeleton className="h-10 w-full" />
                  <Skeleton className="h-10 w-full" />
                </div>
              </div>
            ))}
          </div>
        ) : error ? (
          <div className="rounded-2xl border border-destructive/20 bg-destructive/5 p-8 flex flex-col items-center justify-center text-center space-y-4">
            <AlertCircle className="w-12 h-12 text-destructive/50" />
            <div className="space-y-1">
              <h3 className="font-semibold text-destructive text-lg">Failed to load trends</h3>
              <p className="text-muted-foreground">The API might be unavailable. Please try again later.</p>
            </div>
          </div>
        ) : !trends || !Array.isArray(trends) || trends.length === 0 ? (
          <div className="rounded-2xl border border-border/50 bg-card p-16 flex flex-col items-center justify-center text-center space-y-4">
            <div className="h-16 w-16 rounded-full bg-muted flex items-center justify-center mb-2">
              <UtensilsCrossed className="w-8 h-8 text-muted-foreground/50" />
            </div>
            <h3 className="font-display font-semibold text-xl">No trends detected yet</h3>
            <p className="text-muted-foreground max-w-sm">
              Head over to the Analysis Pipeline to run a new extraction job across social platforms.
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 auto-rows-fr">
            {trends.map((trend, index) => (
              <TrendCard
                key={trend.id}
                trend={trend}
                index={index}
                onSelect={() => setSelectedTrendName(trend.name)}
              />
            ))}
          </div>
        )}

      </div>
    </div>
  );
}
