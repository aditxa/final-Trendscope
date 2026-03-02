import { z } from "zod";
import { insertTrendSchema, trends } from "./schema";

export const errorSchemas = {
  validation: z.object({
    message: z.string(),
    field: z.string().optional(),
  }),
  notFound: z.object({
    message: z.string(),
  }),
  internal: z.object({
    message: z.string(),
  }),
};

export const api = {
  trends: {
    list: {
      method: "GET" as const,
      path: "/api/trends" as const,
      responses: {
        200: z.array(z.custom<typeof trends.$inferSelect>()),
      },
    },
    runPipeline: {
      method: "POST" as const,
      path: "/api/trends/pipeline" as const,
      input: z.object({
        source: z.string(),
      }),
      responses: {
        200: z.object({
          message: z.string(),
          newTrends: z.array(z.custom<typeof trends.$inferSelect>()),
        }),
      },
    },
    googleInterest: {
      method: "GET" as const,
      path: "/api/trends/google-interest" as const,
      // Query params are encoded into the URL; we validate the response only.
      responses: {
        200: z.object({
          name: z.string(),
          points: z.array(
            z.object({
              date: z.string(),
              value: z.number(),
            }),
          ),
        }),
      },
    },
  },
};

export function buildUrl(
  path: string,
  params?: Record<string, string | number>,
): string {
  let url = path;
  if (params) {
    const usp = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      usp.append(key, String(value));
    });
    if ([...usp.keys()].length > 0) {
      url += `?${usp.toString()}`;
    }
  }
  return url;
}
