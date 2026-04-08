import { storage } from "../storage";
import type { InsertTrend, Trend } from "@shared/schema";
import { exec } from "child_process";
import { promisify } from "util";
import path from "path";
import fs from "fs/promises";
import { enrichTrend } from "../services/gemini";

const execAsync = promisify(exec);

export async function ingestGdeltMockTrends(onProgress?: (step: number) => void): Promise<Trend[]> {
  console.log("[GDELT Pipeline] Starting true python ML execution (this takes ~30-60s)...");
  
  // Use the venv python if available, otherwise fallback to system python
  const venvPython = path.join(process.cwd(), "gdelt", ".venv", "Scripts", "python.exe");
  const fallbackPython = "python";
  
  let pythonCmd = fallbackPython;
  try {
    await fs.access(venvPython);
    pythonCmd = venvPython;
  } catch {
    console.log("[GDELT Pipeline] Local venv not found, using global python.");
  }

  const mainModule = "gdelt.src.main";
  const exportPath = path.join(process.cwd(), "data", "gdelt_export.json");

  // Step 0: Scraping Social Data
  onProgress?.(0);
  console.log(`[GDELT Pipeline] Executing: run_once`);
  try {
    await execAsync(`"${pythonCmd}" -m ${mainModule} run_once --minutes 1440`, { cwd: process.cwd(), maxBuffer: 1024 * 1024 * 10 });
  } catch (e) {
    console.error(`[GDELT Pipeline Error on cmd: run_once]`, e);
  }

  // Step 1: Running NLP Trend Detection
  onProgress?.(1);
  const nlpCmds = [
    `"${pythonCmd}" -m ${mainModule} build_counts --bucket daily`,
    `"${pythonCmd}" -m ${mainModule} detect --bucket daily --current-window-hours 24 --baseline-days 0 --min-baseline-samples 0 --top-k 5`,
    `"${pythonCmd}" -m ${mainModule} export --format json --output "${exportPath}" --top-k 5`,
  ];
  for (const cmd of nlpCmds) {
    console.log(`[GDELT Pipeline] Executing: ${cmd}`);
    try {
      await execAsync(cmd, { cwd: process.cwd(), maxBuffer: 1024 * 1024 * 10 });
    } catch (e) {
      console.error(`[GDELT Pipeline Error on cmd: ${cmd}]`, e);
    }
  }

  // Read results
  let parsed: any[] = [];
  try {
    const rawData = await fs.readFile(exportPath, "utf-8");
    parsed = JSON.parse(rawData);
  } catch (err) {
    console.warn("[GDELT Pipeline] Failed to read export JSON. Likely no trends found.", err);
    return [];
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    console.log("[GDELT Pipeline] No trends extracted in this window.");
    return [];
  }

  // Step 2: Validating and Enriching
  onProgress?.(2);
  const existing = await storage.getTrends();
  const existingNames = new Set(existing.map((t) => t.name.toLowerCase()));

  const newTrends: Trend[] = [];

  for (const rawTrend of parsed) {
    const phrase: string = rawTrend.phrase;
    const currentCount: number = rawTrend.current_count;

    if (!phrase || existingNames.has(phrase.toLowerCase())) {
      console.log(`[GDELT Pipeline] Skipping duplicate: ${phrase}`);
      continue;
    }

    console.log(`[GDELT Pipeline] Calling Gemini to enrich phrase: ${phrase}`);
    const enrichment = await enrichTrend(phrase);

    // Capitalize properly
    const finalName = phrase.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

    const insertPayload: InsertTrend = {
      name: finalName,
      originalDish: enrichment.originalDish,
      description: enrichment.description,
      socialVolume: currentCount,
      searchVolume: Math.floor(Math.random() * 50000) + 10000, // Still randomized since google-trends runs live on click
      isEmerging: true,
      source: "GDELT News ML Pipeline",
      indianAlternative: enrichment.indianAlternative,
    };

    const created = await storage.createTrend(insertPayload);
    newTrends.push(created);
    existingNames.add(phrase.toLowerCase());
  }

  console.log(`[GDELT Pipeline] Complete. Added ${newTrends.length} new items.`);
  return newTrends;
}
