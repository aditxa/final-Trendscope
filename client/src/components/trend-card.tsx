import { Trend } from "@shared/schema";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, MessageSquare, Search, ArrowRight, Sparkles, ChefHat } from "lucide-react";
import { motion } from "framer-motion";

interface TrendCardProps {
  trend: Trend;
  index: number;
}

export function TrendCard({ trend, index }: TrendCardProps) {
  // Format large numbers
  const formatNumber = (num: number | null) => {
    if (num === null) return "0";
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'k';
    return num.toString();
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: index * 0.05, ease: "easeOut" }}
    >
      <Card className="group h-full flex flex-col overflow-hidden border border-border/60 hover-elevate bg-card">
        <div className="p-6 flex-1 flex flex-col">
          <div className="flex justify-between items-start mb-4">
            <div className="space-y-1.5">
              <div className="flex items-center gap-2 flex-wrap">
                <h3 className="font-display font-bold text-xl leading-tight text-foreground">
                  {trend.name}
                </h3>
                {trend.isEmerging && (
                  <Badge variant="secondary" className="bg-emerald-50 text-emerald-700 border-emerald-200/50 flex items-center gap-1 hover:bg-emerald-100 transition-colors">
                    <TrendingUp size={12} className="stroke-[2.5]" />
                    <span>Emerging</span>
                  </Badge>
                )}
              </div>
              <p className="text-sm text-muted-foreground/80 font-medium">
                Originated as: <span className="text-foreground/80">{trend.originalDish || 'Unknown'}</span>
              </p>
            </div>
            <Badge variant="outline" className="text-xs uppercase tracking-wider font-semibold text-muted-foreground bg-muted/30">
              {trend.source}
            </Badge>
          </div>

          <p className="text-sm text-muted-foreground leading-relaxed mb-6 line-clamp-3 flex-1">
            {trend.description}
          </p>

          <div className="grid grid-cols-2 gap-3 mb-6 pt-4 border-t border-border/40">
            <div className="flex flex-col gap-1">
              <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                <MessageSquare size={13} />
                <span>Social Vol.</span>
              </div>
              <span className="font-display font-semibold text-lg">{formatNumber(trend.socialVolume)}</span>
            </div>
            <div className="flex flex-col gap-1">
              <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                <Search size={13} />
                <span>Search Vol.</span>
              </div>
              <span className="font-display font-semibold text-lg">{formatNumber(trend.searchVolume)}</span>
            </div>
          </div>
        </div>

        {/* Indian Alternative Section - Highlighted differently */}
        <div className="bg-primary/slate-50 p-5 mt-auto border-t border-border/50 relative overflow-hidden group-hover:bg-primary/[0.02] transition-colors">
          <div className="absolute -right-4 -top-4 text-primary/5 rotate-12 transform group-hover:scale-110 transition-transform duration-500">
            <Sparkles size={64} />
          </div>
          <div className="relative z-10 flex flex-col gap-1.5">
            <div className="flex items-center gap-1.5 text-xs font-bold uppercase tracking-wider text-primary/70">
              <ChefHat size={14} />
              <span>Foodoscope Suggestion</span>
            </div>
            <div className="flex items-start gap-2">
              <ArrowRight size={16} className="text-primary/40 mt-1 shrink-0" />
              <p className="text-sm font-medium text-foreground/90">
                {trend.indianAlternative ? (
                  <>Use <span className="font-bold text-primary">{trend.indianAlternative}</span> as the indigenous alternative.</>
                ) : (
                  <span className="text-muted-foreground italic">Analyzing alternatives...</span>
                )}
              </p>
            </div>
          </div>
        </div>
      </Card>
    </motion.div>
  );
}
