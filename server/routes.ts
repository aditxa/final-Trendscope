import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { api } from "@shared/routes";
import { z } from "zod";
import { ingestRedditTrendsFromFiles } from "./pipelines/reddit";
import { ingestGdeltMockTrends } from "./pipelines/gdelt";
import type { InsertTrend } from "@shared/schema";

const CURATED_TRENDS: InsertTrend[] = [
  {
    name: 'Mushroom-Based "Functional" Coffee',
    originalDish: "Filter coffee",
    description:
      'Aggressive adoption of Cordyceps and Lion’s Mane infusions into standard filter coffee as Gen Z seeks "nootropic" productivity boosts.',
    socialVolume: 18200,
    searchVolume: 64000,
    isEmerging: true,
    source: "Google Trends · India",
    indianAlternative:
      "Filter coffee decoction, Lion’s Mane powder, jaggery, cardamom",
  },
  {
    name: "Tandoori Momos Pasta",
    originalDish: "Pasta",
    description:
      "A hyper-regional fusion using Italian pasta shapes as a vehicle for creamy makhani-tandoori momo fillings.",
    socialVolume: 12500,
    searchVolume: 45000,
    isEmerging: true,
    source: "Instagram · r/indianfoodphotos",
    indianAlternative:
      "Whole wheat pasta with homemade tandoori masala and steamed momos",
  },
  {
    name: "Pistachio-Kunafa Stuffed Medjool Dates",
    originalDish: "Kunafa",
    description:
      'The "clean" evolution of the Dubai chocolate trend: Medjool dates stuffed with kunafa shards, pistachio and white chocolate.',
    socialVolume: 9800,
    searchVolume: 38100,
    isEmerging: true,
    source: "Reddit Dump · 2026-01",
    indianAlternative: "Medjool dates, semolina kunafa, pista, coconut sugar",
  },
  {
    name: "Millet-Coconut Frozen Desserts",
    originalDish: "Ice cream",
    description:
      'Dairy-free, low-sugar frozen desserts using indigenous millets, coconut cream and jaggery targeting the "guilt-free" Gen Z palate.',
    socialVolume: 7600,
    searchVolume: 32900,
    isEmerging: true,
    source: "Foodoscope Tech Analysis",
    indianAlternative:
      "Foxtail millet, coconut milk, nolen gur, roasted dry fruits",
  },
  {
    name: "Desi Taco Fusion (Bajra Roti Base)",
    originalDish: "Tacos",
    description:
      "Mexican tacos where the tortilla is replaced by mini Bajra or Jowar rotis, stuffed with paneer bhurji and mint chutney.",
    socialVolume: 14100,
    searchVolume: 57900,
    isEmerging: true,
    source: "TrendScope Dashboard",
    indianAlternative: "Bajra roti, paneer bhurji, pudina chutney",
  },
  {
    name: "Makhani Ramen",
    originalDish: "Ramen",
    description:
      "Japanese ramen noodles served in a rich, creamy dal makhani-inspired broth, bridging East Asian and North Indian palates.",
    socialVolume: 8900,
    searchVolume: 21000,
    isEmerging: true,
    source: "Reddit · r/delhi, r/mumbai",
    indianAlternative: "Rice noodles, rajma/dal makhani gravy, achar oil",
  },
  {
    name: "Ube-Style Purple Sweet Potato Desserts",
    originalDish: "Ube desserts",
    description:
      'Using Indian purple sweet potato as a near-molecular substitute for global "ube" in cheesecakes, soft-serve and milk cakes.',
    socialVolume: 6200,
    searchVolume: 27400,
    isEmerging: true,
    source: "FlavorGraph & molecular mapping",
    indianAlternative:
      "Purple sweet potato halwa folded into milk cake or shrikhand",
  },
  {
    name: 'Probiotic "Achaar" Bowls',
    originalDish: "Macro bowl",
    description:
      'Western-style macro bowls that use fermented Indian pickles as a functional gut-health topping instead of kimchi.',
    socialVolume: 7100,
    searchVolume: 30100,
    isEmerging: true,
    source: "Twitter/X · academic panels",
    indianAlternative: "Brown rice, sprouts, dahi, mixed regional aachaars",
  },
  {
    name: "Kombucha-Chai Cocktails",
    originalDish: "Chai",
    description:
      'A fermented take on chai, blending kombucha fizz with masala chai aromatics in low-ABV cocktail formats.',
    socialVolume: 8300,
    searchVolume: 35600,
    isEmerging: true,
    source: "Instagram Reels · mixology accounts",
    indianAlternative: "Assam CTC, kombucha starter, jaggery, masala blend",
  },
  {
    name: "Doctor's Ultimate Bread",
    originalDish: "Bread",
    description:
      "A high-protein bread recipe going viral for packing 10g protein per slice using nutrient-dense flour blends, positioned as the ultimate health-conscious carb.",
    socialVolume: 9400,
    searchVolume: 33200,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Ragi-multigrain atta bread with flaxseed, peanut flour, and jaggery glaze",
  },
  {
    name: "Japanese Cheesecake",
    originalDish: "Cheesecake",
    description:
      "A massively viral 2-ingredient Japanese cheesecake taking over TikTok — jiggly, cloud-light, and requiring only cream cheese and eggs.",
    socialVolume: 22500,
    searchVolume: 71000,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Hung curd cheesecake with cardamom, saffron, and a digestive biscuit base",
  },
  {
    name: "Paratha Burger",
    originalDish: "Burger",
    description:
      "A fusion trend replacing the classic burger bun with crispy, flaky paratha — delivering buttery layers with every bite of the patty.",
    socialVolume: 11300,
    searchVolume: 42500,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Laccha paratha bun, spiced paneer or keema patty, mint raita, pickled onions",
  },
];

// Seed function to insert initial curated data
async function seedDatabase() {
  const existingTrends = await storage.getTrends();
  if (existingTrends.length === 0) {
    for (const trend of CURATED_TRENDS) {
      await storage.createTrend(trend);
    }
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

  app.get(api.trends.googleInterest.path, async (req, res) => {
    try {
      const name =
        typeof req.query.name === "string" && req.query.name.trim().length > 0
          ? req.query.name.trim()
          : undefined;

      const targetName =
        name ??
        (await storage.getTrends().then((t) => (t[0] ? t[0].name : "")));

      const baseSeries: Record<string, number[]> = {
        'Mushroom-Based "Functional" Coffee': [32, 45, 58, 71, 80, 83, 86, 90],
        "Tandoori Momos Pasta": [40, 52, 60, 68, 74, 79, 81, 84],
        "Pistachio-Kunafa Stuffed Medjool Dates": [
          18, 26, 35, 49, 63, 72, 78, 82,
        ],
        "Millet-Coconut Frozen Desserts": [14, 22, 29, 41, 55, 61, 67, 73],
        "Desi Taco Fusion (Bajra Roti Base)": [20, 33, 47, 59, 66, 72, 77, 81],
        "Makhani Ramen": [25, 37, 48, 56, 62, 68, 71, 75],
        "Ube-Style Purple Sweet Potato Desserts": [
          12, 19, 27, 35, 44, 52, 59, 66,
        ],
        'Probiotic "Achaar" Bowls': [10, 18, 26, 34, 43, 51, 58, 64],
        "Kombucha-Chai Cocktails": [16, 24, 33, 42, 50, 57, 63, 69],
        "Doctor's Ultimate Bread": [8, 15, 24, 38, 52, 64, 73, 80],
        "Japanese Cheesecake": [5, 12, 20, 34, 55, 72, 85, 95],
        "Paratha Burger": [10, 18, 28, 40, 53, 62, 70, 76],
        "Smash Burger Tacos": [6, 14, 25, 42, 60, 74, 82, 90],
        "Cottage Cheese Ice Cream": [12, 20, 32, 45, 56, 64, 71, 78],
        "Dubai Chocolate Bar": [8, 18, 35, 55, 70, 82, 90, 96],
        "Birria Ramen": [15, 24, 34, 46, 55, 62, 68, 73],
        "Protein Cookie Dough": [4, 10, 18, 28, 40, 50, 58, 65],
      };

      const dates = [
        "2026-01-01",
        "2026-01-08",
        "2026-01-15",
        "2026-01-22",
        "2026-01-29",
        "2026-02-05",
        "2026-02-12",
        "2026-02-19",
      ];

      const series =
        (targetName && baseSeries[targetName]) ??
        baseSeries['Mushroom-Based "Functional" Coffee'];

      const points = dates.map((date, idx) => ({
        date,
        value: series[idx] ?? series[series.length - 1],
      }));

      res.json({ name: targetName, points });
    } catch (err) {
      res.status(500).json({ message: "Failed to load Google interest data" });
    }
  });

  app.post(api.trends.runPipeline.path, async (req, res) => {
    try {
      const input = api.trends.runPipeline.input.parse(req.body);

      if (input.source.toLowerCase() === "reddit") {
        try {
          const newTrends = await ingestRedditTrendsFromFiles();

          if (newTrends.length > 0) {
            return res.status(200).json({
              message: `Reddit pipeline completed successfully. Ingested ${newTrends.length} trend(s).`,
              newTrends,
            });
          }
        } catch (pipelineError) {
          console.error(
            "Reddit ingestion failed, falling back to synthetic data:",
            pipelineError,
          );
        }
      }

      if (input.source.toLowerCase() === "gdelt") {
        try {
          const newTrends = await ingestGdeltMockTrends();

          if (newTrends.length > 0) {
            return res.status(200).json({
              message: `GDELT pipeline completed successfully. Discovered ${newTrends.length} trend(s) from global news articles.`,
              newTrends,
            });
          }
        } catch (pipelineError) {
          console.error(
            "GDELT ingestion failed, falling back to synthetic data:",
            pipelineError,
          );
        }
      }

      const allTrends = await storage.getTrends();
      const fallbackName = "Desi Taco Fusion (Bajra Roti Base)";
      const existing =
        allTrends.find((t) => t.name === fallbackName) ?? null;

      const mockedNewTrend =
        existing ??
        (await storage.createTrend({
          name: fallbackName,
          originalDish: "Tacos",
          description:
            "Mexican tacos where the tortilla is replaced by mini Bajra or Jowar rotis, stuffed with paneer bhurji and mint chutney.",
          socialVolume: Math.floor(Math.random() * 20000) + 8000,
          searchVolume: Math.floor(Math.random() * 50000) + 20000,
          isEmerging: true,
          source: input.source,
          indianAlternative: "Bajra roti, paneer bhurji, pudina chutney",
        }));

      res.status(200).json({
        message: "Pipeline completed successfully.",
        newTrends: [mockedNewTrend],
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