import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useRecipe } from "@/hooks/use-recipe";
import { Trend } from "@shared/schema";
import { Loader2, ChefHat, Sparkles, AlertCircle, Info } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";

interface RecipeModalProps {
  trend: Trend | null;
  isOpen: boolean;
  onClose: () => void;
}

export function RecipeModal({ trend, isOpen, onClose }: RecipeModalProps) {
  const { data: recipe, isLoading, error } = useRecipe(trend?.id);

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-3xl max-h-[85vh] overflow-y-auto bg-card border-border/50 text-foreground p-0 gap-0 shadow-2xl">
        <DialogHeader className="p-6 md:p-8 bg-muted/20 border-b border-border/40 sticky top-0 z-10 backdrop-blur-md">
          <DialogTitle className="flex flex-col gap-2">
            <span className="flex items-center gap-2 text-primary font-bold text-sm uppercase tracking-wider">
              <Sparkles size={16} />Recipe
            </span>
            <span className="text-3xl font-display">{trend?.name}</span>
          </DialogTitle>
          <DialogDescription className="text-base text-muted-foreground mt-2">
            {trend?.description}
          </DialogDescription>
        </DialogHeader>

        <div className="p-6 md:p-8">
          <AnimatePresence mode="wait">
            {isLoading ? (
              <motion.div
                key="loading"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex flex-col items-center justify-center py-20 gap-4"
              >
                <div className="relative flex items-center justify-center">
                  <div className="absolute inset-0 bg-primary/20 rounded-full blur-xl animate-pulse" />
                  <Loader2 className="w-12 h-12 text-primary animate-spin relative z-10" />
                </div>
                <p className="font-semibold text-lg text-foreground/80">Crafting your recipe...</p>
                <p className="text-sm text-muted-foreground text-center max-w-sm">
                  Analyzing ingredients for Indian alternatives using Gemini AI context reasoning.
                </p>
              </motion.div>
            ) : error ? (
              <motion.div
                key="error"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex flex-col items-center justify-center p-12 text-center text-destructive"
              >
                <AlertCircle className="w-16 h-16 mb-4 opacity-50" />
                <h3 className="font-display font-medium text-xl">Generation Failed</h3>
                <p className="text-sm opacity-80 mt-2">Could not generate the recipe at this time.</p>
              </motion.div>
            ) : recipe ? (
              <motion.div
                key="recipe"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5 }}
                className="space-y-10"
              >
                {/* Ingredients Section */}
                <section>
                  <h3 className="text-xl font-display font-bold mb-5 flex items-center gap-2">
                    <ChefHat className="text-primary" /> Ingredients
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {(recipe.ingredients as any[]).map((ingredient: any, idx: number) => (
                      <div
                        key={idx}
                        className={`p-4 rounded-xl border ${ingredient.isHardToSource
                            ? "bg-amber-500/5 border-amber-500/20"
                            : "bg-muted/30 border-border/50"
                          } flex flex-col gap-2 transition-colors`}
                      >
                        <div className="flex justify-between items-start">
                          <span className="font-semibold">{ingredient.name}</span>
                          <span className="text-muted-foreground text-sm font-medium bg-background px-2 py-1 rounded-md shadow-sm border border-border/40">
                            {ingredient.amount}
                          </span>
                        </div>

                        {ingredient.isHardToSource && ingredient.indianAlternative && (
                          <div className="mt-2 text-sm bg-amber-500/10 text-amber-700 p-2.5 rounded-lg flex items-start gap-2 border border-amber-500/20">
                            <Info size={16} className="shrink-0 mt-0.5" />
                            <div>
                              <span className="block font-semibold mb-0.5">Indian Alternative:</span>
                              {ingredient.indianAlternative}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </section>

                {/* Steps Section */}
                <section>
                  <h3 className="text-xl font-display font-bold mb-6 flex items-center gap-2">
                    <span className="flex items-center justify-center w-6 h-6 rounded-md bg-primary text-primary-foreground text-xs">
                      1..
                    </span>
                    Method
                  </h3>
                  <div className="space-y-6">
                    {(recipe.steps as string[]).map((step: string, idx: number) => (
                      <div key={idx} className="flex gap-4 group">
                        <div className="flex flex-col items-center">
                          <span className="flex items-center justify-center w-8 h-8 rounded-full bg-muted font-bold text-sm text-foreground group-hover:bg-primary group-hover:text-primary-foreground transition-colors shrink-0">
                            {idx + 1}
                          </span>
                          {idx !== (recipe.steps as string[]).length - 1 && (
                            <div className="w-px h-full bg-border mt-2 group-hover:bg-primary/20 transition-colors" />
                          )}
                        </div>
                        <p className="text-foreground/80 leading-relaxed pt-1 pb-4">
                          {step}
                        </p>
                      </div>
                    ))}
                  </div>
                </section>
              </motion.div>
            ) : null}
          </AnimatePresence>
        </div>
      </DialogContent>
    </Dialog>
  );
}
