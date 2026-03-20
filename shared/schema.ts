import { pgTable, text, serial, boolean, integer, timestamp, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const trends = pgTable("trends", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  originalDish: text("original_dish"),
  description: text("description").notNull(),
  socialVolume: integer("social_volume").default(0),
  searchVolume: integer("search_volume").default(0),
  isEmerging: boolean("is_emerging").default(false),
  source: text("source").notNull(), 
  indianAlternative: text("indian_alternative"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const recipes = pgTable("recipes", {
  id: serial("id").primaryKey(),
  trendId: integer("trend_id").references(() => trends.id).notNull(),
  ingredients: jsonb("ingredients").notNull(),
  steps: jsonb("steps").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertTrendSchema = createInsertSchema(trends).omit({ id: true, createdAt: true });
export const insertRecipeSchema = createInsertSchema(recipes).omit({ id: true, createdAt: true });

export type Trend = typeof trends.$inferSelect;
export type InsertTrend = z.infer<typeof insertTrendSchema>;

export type Recipe = typeof recipes.$inferSelect;
export type InsertRecipe = z.infer<typeof insertRecipeSchema>;