import { useQuery } from "@tanstack/react-query";
import { Recipe } from "@shared/schema";
import { api, buildUrl } from "@shared/routes";

export function useRecipe(trendId?: number) {
  return useQuery<Recipe>({
    queryKey: ["recipe", trendId],
    queryFn: async () => {
      if (!trendId) throw new Error("No trendId provided");
      
      const res = await fetch(`/api/trends/${trendId}/recipe`);
      if (!res.ok) {
        throw new Error("Failed to fetch or generate recipe");
      }
      return res.json();
    },
    enabled: !!trendId,
  });
}
