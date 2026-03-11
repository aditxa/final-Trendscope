/**
 * Mock GDELT news-trend pipeline.
 *
 * In a real deployment, this would shell out to the Python `gdelt/` package
 * (e.g. `python -m gdelt.src.main detect …`) and read the resulting trends
 * table.  For the MVP dashboard, we return a curated set of food trends that
 * mirror what the real pipeline would surface from GDELT DOC 2.0 articles.
 */

import { storage } from "../storage";
import type { InsertTrend, Trend } from "@shared/schema";

const GDELT_MOCK_TRENDS: InsertTrend[] = [
  {
    name: "Smash Burger Tacos",
    originalDish: "Tacos",
    description:
      "A viral mashup that presses seasoned ground beef into a tortilla on a flat griddle, creating a crispy cheese-skirted taco that dominated GDELT news volume in February 2026.",
    socialVolume: 19400,
    searchVolume: 68000,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Chapati smash taco with keema, amul cheese crust, and green chutney",
  },
  {
    name: "Cottage Cheese Ice Cream",
    originalDish: "Ice cream",
    description:
      "High-protein frozen dessert blending cottage cheese, frozen fruit and honey — championed by fitness creators and picked up by mainstream food media worldwide.",
    socialVolume: 15800,
    searchVolume: 54200,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Paneer-based frozen kulfi with cardamom, saffron, and roasted pistachios",
  },
  {
    name: "Dubai Chocolate Bar",
    originalDish: "Chocolate bar",
    description:
      "Pistachio-kunafa stuffed chocolate bars originating from a Dubai confectioner that went globally viral, spawning hundreds of copycat recipes across news outlets.",
    socialVolume: 31200,
    searchVolume: 92000,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Kaju katli chocolate bark with crushed kunafa, pista, and desi ghee",
  },
  {
    name: "Birria Ramen",
    originalDish: "Ramen",
    description:
      "Mexican birria consommé replaces traditional tonkotsu broth in ramen bowls — a cross-cultural fusion surging through food-media headlines.",
    socialVolume: 13600,
    searchVolume: 47500,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Nihari-style slow-cooked mutton broth ramen with hand-pulled noodles and mirchi oil",
  },
  {
    name: "Protein Cookie Dough",
    originalDish: "Cookie dough",
    description:
      "Edible, no-bake cookie dough fortified with protein powder and chickpea flour — a gym-culture staple that crossed into mainstream news coverage.",
    socialVolume: 10200,
    searchVolume: 38900,
    isEmerging: true,
    source: "GDELT News Trend Pipeline",
    indianAlternative:
      "Besan-jaggery protein ladoo dough with whey, dark chocolate chips, and ghee",
  },
];

/**
 * Simulate running the GDELT pipeline and return newly created trends.
 *
 * De-duplicates against existing trends by name to avoid double-inserts on
 * repeated pipeline runs.
 */
export async function ingestGdeltMockTrends(): Promise<Trend[]> {
  const existing = await storage.getTrends();
  const existingNames = new Set(existing.map((t) => t.name));

  const newTrends: Trend[] = [];

  for (const mock of GDELT_MOCK_TRENDS) {
    if (existingNames.has(mock.name)) {
      // Already exists — return the existing record.
      const found = existing.find((t) => t.name === mock.name);
      if (found) newTrends.push(found);
      continue;
    }

    const created = await storage.createTrend(mock);
    newTrends.push(created);
  }

  return newTrends;
}
