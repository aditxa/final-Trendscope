import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api, buildUrl } from "@shared/routes";

// Utility function to log and throw validation errors
function parseWithLogging<T>(schema: any, data: unknown, label: string): T {
  const result = schema.safeParse(data);
  if (!result.success) {
    console.error(`[Zod] ${label} validation failed:`, result.error.format());
    throw new Error(`Invalid response format from ${label}`);
  }
  return result.data;
}

export function useTrends() {
  return useQuery({
    queryKey: [api.trends.list.path],
    queryFn: async () => {
      const res = await fetch(api.trends.list.path, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch trends");
      const data = await res.json();
      return parseWithLogging(
        api.trends.list.responses[200],
        data,
        "trends.list",
      );
    },
  });
}

export function useTrendInterest(name: string | undefined) {
  return useQuery({
    enabled: !!name,
    queryKey: [api.trends.googleInterest.path, name],
    queryFn: async () => {
      const url = buildUrl(api.trends.googleInterest.path, { name: name! });
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch search interest");
      const data = await res.json();
      return parseWithLogging(
        api.trends.googleInterest.responses[200],
        data,
        "trends.googleInterest",
      );
    },
  });
}

export function useRunPipeline() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (input: { source: string }) => {
      const validatedInput = api.trends.runPipeline.input.parse(input);

      const res = await fetch(api.trends.runPipeline.path, {
        method: api.trends.runPipeline.method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(validatedInput),
        credentials: "include",
      });

      if (!res.ok) {
        throw new Error("Failed to run pipeline");
      }

      const data = await res.json();
      return parseWithLogging(
        api.trends.runPipeline.responses[200],
        data,
        "trends.pipeline",
      );
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [api.trends.list.path] });
    },
  });
}

