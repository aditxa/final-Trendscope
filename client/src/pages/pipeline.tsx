import { useState, useEffect } from "react";
import { useRunPipeline } from "@/hooks/use-trends";
import { useToast } from "@/hooks/use-toast";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { 
  Network, 
  Terminal, 
  CheckCircle2, 
  Play, 
  Database,
  BrainCircuit,
  LineChart,
  ChefHat
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

const PIPELINE_STEPS = [
  { id: 'scrape', label: 'Scraping Social Data', icon: Database },
  { id: 'nlp', label: 'Running NLP Trend Detection', icon: BrainCircuit },
  { id: 'validate', label: 'Validating with Google Trends', icon: LineChart },
  { id: 'foodoscope', label: 'Querying Foodoscope API', icon: ChefHat },
];

export default function Pipeline() {
  const [source, setSource] = useState("reddit");
  const [activeStepIndex, setActiveStepIndex] = useState(-1);
  const { mutate, isPending } = useRunPipeline();
  const { toast } = useToast();

  // Simulate progress through steps while mutation is pending
  useEffect(() => {
    let interval: NodeJS.Timeout;
    
    if (isPending) {
      setActiveStepIndex(0);
      let currentIndex = 0;
      
      // Advance step every 2 seconds to simulate work
      interval = setInterval(() => {
        currentIndex++;
        if (currentIndex < PIPELINE_STEPS.length) {
          setActiveStepIndex(currentIndex);
        }
      }, 2000);
    } else {
      setActiveStepIndex(-1);
    }

    return () => clearInterval(interval);
  }, [isPending]);

  const handleRunPipeline = () => {
    mutate({ source }, {
      onSuccess: (data) => {
        toast({
          title: "Pipeline Completed Successfully",
          description: data.message || `Discovered ${data.newTrends.length} new trends.`,
          variant: "default",
        });
      },
      onError: (err) => {
        toast({
          title: "Pipeline Execution Failed",
          description: err.message,
          variant: "destructive",
        });
        setActiveStepIndex(-1);
      }
    });
  };

  return (
    <div className="min-h-screen bg-background/50 p-6 md:p-8 lg:p-10 flex items-center justify-center">
      <div className="w-full max-w-3xl space-y-8">
        
        <div className="text-center space-y-3 mb-10">
          <div className="inline-flex h-12 w-12 items-center justify-center rounded-2xl bg-primary/10 text-primary mb-2">
            <Network size={24} className="stroke-[2]" />
          </div>
          <h1 className="font-display text-3xl md:text-4xl font-bold tracking-tight">
            Analysis Engine
          </h1>
          <p className="text-muted-foreground text-lg max-w-xl mx-auto">
            Configure and deploy extraction agents to discover emerging culinary patterns.
          </p>
        </div>

        <Card className="border border-border/60 shadow-lg shadow-black/5 overflow-hidden bg-card">
          <div className="p-8 md:p-10">
            <div className="grid gap-8 md:grid-cols-[1fr_1.5fr]">
              
              {/* Left Column: Configuration */}
              <div className="space-y-6">
                <div>
                  <h3 className="font-display font-semibold text-lg mb-1">Configuration</h3>
                  <p className="text-sm text-muted-foreground">Set parameters for the extraction job.</p>
                </div>
                
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="source" className="text-xs uppercase tracking-wider font-semibold text-muted-foreground">
                      Target Data Source
                    </Label>
                    <Select 
                      value={source} 
                      onValueChange={setSource}
                      disabled={isPending}
                    >
                      <SelectTrigger id="source" className="h-12 bg-background border-border/50 focus:ring-primary/20 transition-all">
                        <SelectValue placeholder="Select platform" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="reddit">Reddit Subforums</SelectItem>
                        <SelectItem value="gdelt">GDELT News Articles</SelectItem>
                        <SelectItem value="discord">Discord Communities</SelectItem>
                        <SelectItem value="twitter">X (Twitter) Firehose</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                <div className="pt-4">
                  <Button 
                    onClick={handleRunPipeline} 
                    disabled={isPending}
                    className="w-full h-12 text-base font-semibold shadow-md hover:shadow-lg transition-all"
                  >
                    {isPending ? (
                      <>
                        <Terminal className="mr-2 h-5 w-5 animate-pulse" />
                        Executing Pipeline...
                      </>
                    ) : (
                      <>
                        <Play className="mr-2 h-5 w-5" />
                        Deploy Extraction Agent
                      </>
                    )}
                  </Button>
                </div>
              </div>

              {/* Right Column: Visualizer */}
              <div className="bg-muted/30 rounded-xl p-6 border border-border/40 relative min-h-[300px] flex flex-col justify-center">
                <div className="absolute top-4 left-4 flex items-center gap-2 opacity-50">
                  <Terminal size={14} />
                  <span className="text-xs font-mono tracking-wider">PROCESS_LOG</span>
                </div>

                {!isPending && activeStepIndex === -1 ? (
                  <div className="text-center text-muted-foreground space-y-3 opacity-60 flex flex-col items-center justify-center py-10">
                    <BrainCircuit size={48} className="stroke-[1] mb-2" />
                    <p className="text-sm font-medium">System Idle</p>
                    <p className="text-xs">Awaiting configuration and deployment command.</p>
                  </div>
                ) : (
                  <div className="space-y-6 pt-6 pl-4 relative">
                    {/* Vertical connecting line */}
                    <div className="absolute left-[11px] top-10 bottom-4 w-[2px] bg-border/50 rounded-full" />
                    
                    {PIPELINE_STEPS.map((step, index) => {
                      const isActive = index === activeStepIndex;
                      const isComplete = index < activeStepIndex;
                      const isPending = index > activeStepIndex;
                      const StepIcon = step.icon;

                      return (
                        <motion.div 
                          key={step.id}
                          initial={{ opacity: 0, x: -10 }}
                          animate={{ opacity: 1, x: 0 }}
                          transition={{ delay: index * 0.1 }}
                          className={`flex items-start gap-4 relative z-10 transition-all duration-300 ${
                            isActive ? 'scale-105 origin-left' : ''
                          }`}
                        >
                          <div className={`
                            flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center
                            transition-colors duration-500 shadow-sm
                            ${isActive ? 'bg-primary text-primary-foreground ring-4 ring-primary/20' : 
                              isComplete ? 'bg-emerald-500 text-white' : 
                              'bg-background border-2 border-muted-foreground/30 text-muted-foreground/30'}
                          `}>
                            {isComplete ? (
                              <CheckCircle2 size={14} className="stroke-[3]" />
                            ) : (
                              <StepIcon size={12} className={isActive ? "animate-pulse" : ""} />
                            )}
                          </div>
                          
                          <div className={`pt-0.5 space-y-1 transition-colors duration-300 ${
                            isActive ? 'text-foreground font-semibold' : 
                            isComplete ? 'text-foreground/80' : 
                            'text-muted-foreground/50'
                          }`}>
                            <p className="text-sm tracking-tight">{step.label}</p>
                            {isActive && (
                              <motion.div 
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: 'auto' }}
                                className="text-xs text-muted-foreground font-mono"
                              >
                                {index === 0 && "> Establishing secure connection..."}
                                {index === 1 && "> Processing semantic patterns..."}
                                {index === 2 && "> Correlating search indices..."}
                                {index === 3 && "> Generating ingredient mappings..."}
                              </motion.div>
                            )}
                          </div>
                        </motion.div>
                      );
                    })}
                  </div>
                )}
              </div>

            </div>
          </div>
        </Card>
        
        {/* Subtle decorative background elements */}
        {/* landing page abstract architectural background light */}
        <div className="fixed inset-0 z-[-1] overflow-hidden pointer-events-none opacity-[0.03]">
          <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] rounded-full bg-primary blur-[120px]" />
          <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] rounded-full bg-primary blur-[120px]" />
        </div>
      </div>
    </div>
  );
}
