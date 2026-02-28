import { db } from "./db";
import { trends, type InsertTrend, type Trend } from "@shared/schema";
import { eq } from "drizzle-orm";

export interface IStorage {
  getTrends(): Promise<Trend[]>;
  getTrend(id: number): Promise<Trend | undefined>;
  createTrend(trend: InsertTrend): Promise<Trend>;
}

export class DatabaseStorage implements IStorage {
  async getTrends(): Promise<Trend[]> {
    return await db.select().from(trends);
  }

  async getTrend(id: number): Promise<Trend | undefined> {
    const [trend] = await db.select().from(trends).where(eq(trends.id, id));
    return trend;
  }

  async createTrend(insertTrend: InsertTrend): Promise<Trend> {
    const [trend] = await db.insert(trends).values(insertTrend).returning();
    return trend;
  }
}

export const storage = new DatabaseStorage();