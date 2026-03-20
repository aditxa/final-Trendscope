import { promises as fs } from "node:fs";
import { existsSync } from "node:fs";
import path from "node:path";
import { db } from "./db";
import { trends, recipes, type InsertTrend, type Trend, type InsertRecipe, type Recipe } from "@shared/schema";
import { eq } from "drizzle-orm";

export interface IStorage {
  getTrends(): Promise<Trend[]>;
  getTrend(id: number): Promise<Trend | undefined>;
  createTrend(trend: InsertTrend): Promise<Trend>;
  getRecipe(trendId: number): Promise<Recipe | undefined>;
  createRecipe(recipe: InsertRecipe): Promise<Recipe>;
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

  async getRecipe(trendId: number): Promise<Recipe | undefined> {
    const [recipe] = await db.select().from(recipes).where(eq(recipes.trendId, trendId));
    return recipe;
  }

  async createRecipe(insertRecipe: InsertRecipe): Promise<Recipe> {
    const [recipe] = await db.insert(recipes).values(insertRecipe).returning();
    return recipe;
  }
}

/**
 * Lightweight JSON-file-backed storage used when no DATABASE_URL / Postgres
 * is available. This keeps the app fully functional without external infra.
 */
export class FileStorage implements IStorage {
  private readonly dataFile: string;
  private readonly recipesFile: string;

  constructor(filePath?: string) {
    this.dataFile =
      filePath ??
      process.env.TRENDS_JSON_PATH ??
      path.join(process.cwd(), "data", "trends.json");
    this.recipesFile = path.join(process.cwd(), "data", "recipes.json");
  }

  private async ensureDir(): Promise<void> {
    const dir = path.dirname(this.dataFile);
    await fs.mkdir(dir, { recursive: true });
  }

  private async readAll(): Promise<Trend[]> {
    if (!existsSync(this.dataFile)) {
      return [];
    }
    const raw = await fs.readFile(this.dataFile, "utf-8");
    if (!raw.trim()) return [];
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed as Trend[];
      }
      return [];
    } catch {
      return [];
    }
  }

  private async writeAll(trendsList: Trend[]): Promise<void> {
    await this.ensureDir();
    await fs.writeFile(
      this.dataFile,
      JSON.stringify(trendsList, null, 2),
      "utf-8"
    );
  }

  async getTrends(): Promise<Trend[]> {
    return await this.readAll();
  }

  async getTrend(id: number): Promise<Trend | undefined> {
    const all = await this.readAll();
    return all.find((t) => t.id === id);
  }

  async createTrend(insertTrend: InsertTrend): Promise<Trend> {
    const all = await this.readAll();
    const nextId =
      all.length === 0 ? 1 : Math.max(...all.map((t) => t.id ?? 0)) + 1;

    const now = new Date();

    const trend: Trend = {
      id: nextId,
      name: insertTrend.name,
      description: insertTrend.description,
      originalDish: insertTrend.originalDish ?? null,
      socialVolume: insertTrend.socialVolume ?? 0,
      searchVolume: insertTrend.searchVolume ?? 0,
      isEmerging: insertTrend.isEmerging ?? false,
      source: insertTrend.source,
      indianAlternative: insertTrend.indianAlternative ?? null,
      createdAt: (insertTrend as any).createdAt ?? now,
    };

    all.push(trend);
    await this.writeAll(all);
    return trend;
  }

  private async readAllRecipes(): Promise<Recipe[]> {
    if (!existsSync(this.recipesFile)) return [];
    const raw = await fs.readFile(this.recipesFile, "utf-8");
    if (!raw.trim()) return [];
    try {
      return JSON.parse(raw) as Recipe[];
    } catch {
      return [];
    }
  }

  private async writeAllRecipes(recipesList: Recipe[]): Promise<void> {
    await this.ensureDir();
    await fs.writeFile(this.recipesFile, JSON.stringify(recipesList, null, 2), "utf-8");
  }

  async getRecipe(trendId: number): Promise<Recipe | undefined> {
    const all = await this.readAllRecipes();
    return all.find((r) => r.trendId === trendId);
  }

  async createRecipe(insertRecipe: InsertRecipe): Promise<Recipe> {
    const all = await this.readAllRecipes();
    const nextId = all.length === 0 ? 1 : Math.max(...all.map((r) => r.id ?? 0)) + 1;
    
    const recipe: Recipe = {
      id: nextId,
      trendId: insertRecipe.trendId,
      ingredients: insertRecipe.ingredients,
      steps: insertRecipe.steps,
      createdAt: (insertRecipe as any).createdAt ?? new Date(),
    };

    all.push(recipe);
    await this.writeAllRecipes(all);
    return recipe;
  }
}

// Choose storage implementation based on whether a real database is configured.
export const storage: IStorage =
  db != null ? new DatabaseStorage() : new FileStorage();
