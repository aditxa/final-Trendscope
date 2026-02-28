import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import { z } from "zod";

// Seed function to insert initial mock data
async function seedDatabase() {
  const existingTrends = await storage.getTrends();
  if (existingTrends.length === 0) {
    await storage.createTrend({
      name: "Tandoori Momos Pasta",
      originalDish: "Pasta",
      description: "A fusion dish combining italian pasta with tandoori spices and mini momos.",
      socialVolume: 12500,
      searchVolume: 45000,
      isEmerging: true,
      source: "Reddit",
      indianAlternative: "Whole wheat pasta with homemade tandoori masala"
    });
    
    await storage.createTrend({
      name: "Makhani Ramen",
      originalDish: "Ramen",
      description: "Japanese ramen noodles served in a rich, creamy dal makhani broth.",
      socialVolume: 8900,
      searchVolume: 21000,
      isEmerging: true,
      source: "Discord",
      indianAlternative: "Rice noodles, paneer, makhani gravy"
    });
  }
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Seed the DB on startup
  seedDatabase().catch(console.error);

  app.get(api.trends.list.path, async (req, res) => {
    try {
      const allTrends = await storage.getTrends();
      res.json(allTrends);
    } catch (err) {
      res.status(500).json({ message: "Failed to fetch trends" });
    }
  });

  app.post(api.trends.runPipeline.path, async (req, res) => {
    try {
      const input = api.trends.runPipeline.input.parse(req.body);
      
      // Simulate extreme expert ML, NLP, Data Analysis, and Foodoscope API integration
      // In reality, this would trigger background workers or call Python microservices
      await new Promise(resolve => setTimeout(resolve, 3000)); // Simulate processing delay
      
      const mockedNewTrend = await storage.createTrend({
        name: `Desi Taco Fusion (${input.source})`,
        originalDish: "Tacos",
        description: `Trend detected from ${input.source}. Mexican tacos filled with spicy paneer bhurji and mint chutney.`,
        socialVolume: Math.floor(Math.random() * 20000) + 5000,
        searchVolume: Math.floor(Math.random() * 50000) + 10000,
        isEmerging: true,
        source: input.source,
        indianAlternative: "Bajra roti, Paneer, Pudina"
      });
      
      res.status(200).json({
        message: "Pipeline completed successfully.",
        newTrends: [mockedNewTrend]
      });
    } catch (err) {
      if (err instanceof z.ZodError) {
        return res.status(400).json({
          message: err.errors[0].message,
          field: err.errors[0].path.join('.'),
        });
      }
      res.status(500).json({ message: "Pipeline failed" });
    }
  });

  return httpServer;
}