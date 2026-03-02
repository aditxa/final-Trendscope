import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { existsSync } from "node:fs";
import { storage } from "../storage";
import type { InsertTrend, Trend } from "@shared/schema";

type RedditIngestOptions = {
  submissionsPath?: string;
  commentsPath?: string;
  maxPosts?: number;
  maxComments?: number;
  maxTrends?: number;
};

type TrendAccumulator = {
  key: string;
  name: string;
  description: string;
  socialVolume: number;
  searchVolume: number;
};

const DEFAULT_SUBMISSIONS_PATH =
  process.env.REDDIT_SUBMISSIONS_PATH ??
  "C:\\\\Users\\\\bhand\\\\Downloads\\\\reddit\\\\submissions\\\\RS_2026-01.zst";

const DEFAULT_COMMENTS_PATH =
  process.env.REDDIT_COMMENTS_PATH ??
  "C:\\\\Users\\\\bhand\\\\Downloads\\\\reddit\\\\comments\\\\RC_2026-01.zst";

const FOOD_SUBREDDITS = new Set<string>([
  "food",
  "foodporn",
  "cooking",
  "recipes",
  "indianfood",
  "askculinary",
  "veganrecipes",
  "vegetarian",
  "tonightsdinner",
]);

function normalizeTitle(title: string): string {
  return title.trim().toLowerCase();
}

function ensureZstdAvailableError(binaryName: string, original: unknown): Error {
  const baseMessage =
    `Failed to start '${binaryName}'. ` +
    "Make sure Zstandard (zstd) is installed and available on your PATH. " +
    "On Windows you can install it from the official releases and add zstd.exe to PATH.";

  if (original instanceof Error) {
    return new Error(`${baseMessage} Original error: ${original.message}`);
  }
  return new Error(baseMessage);
}

function streamZstJsonLines(
  filePath: string,
  handler: (obj: any) => boolean | void,
  maxLines?: number
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn("zstd", ["-dc", filePath]);

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (err) => {
      reject(ensureZstdAvailableError("zstd", err));
    });

    let stopped = false;
    let processed = 0;

    const rl = createInterface({
      input: child.stdout,
      crlfDelay: Infinity,
    });

    rl.on("line", (line) => {
      if (stopped) return;

      if (maxLines !== undefined && processed >= maxLines) {
        stopped = true;
        rl.close();
        child.kill();
        return;
      }

      const trimmed = line.trim();
      if (!trimmed) return;

      try {
        const obj = JSON.parse(trimmed);
        processed += 1;
        const shouldContinue = handler(obj);
        if (shouldContinue === false) {
          stopped = true;
          rl.close();
          child.kill();
        }
      } catch {
        // Ignore individual line parse errors and continue streaming
      }
    });

    rl.on("close", () => {
      if (!stopped) {
        child.kill();
      }
      resolve();
    });

    child.on("exit", (code) => {
      if (!stopped && code !== 0) {
        reject(
          new Error(
            `zstd exited with code ${code} while reading ${filePath}. stderr: ${stderr}`
          )
        );
      }
    });
  });
}

export async function ingestRedditTrendsFromFiles(
  options: RedditIngestOptions = {}
): Promise<Trend[]> {
  const submissionsPath = options.submissionsPath ?? DEFAULT_SUBMISSIONS_PATH;
  const commentsPath = options.commentsPath ?? DEFAULT_COMMENTS_PATH;
  const maxPosts = options.maxPosts ?? 50000;
  const maxComments = options.maxComments ?? 200000;
  const maxTrends = options.maxTrends ?? 25;

  if (!existsSync(submissionsPath)) {
    throw new Error(
      `Reddit submissions file not found at '${submissionsPath}'. ` +
        "Update REDDIT_SUBMISSIONS_PATH or move the file to that location."
    );
  }

  if (!existsSync(commentsPath)) {
    throw new Error(
      `Reddit comments file not found at '${commentsPath}'. ` +
        "Update REDDIT_COMMENTS_PATH or move the file to that location."
    );
  }

  const accumulators = new Map<string, TrendAccumulator>();
  const postIdToKey = new Map<string, string>();

  await streamZstJsonLines(
    submissionsPath,
    (obj) => {
      if (!obj || typeof obj !== "object") return;

      const subreddit = String(obj.subreddit ?? "").toLowerCase();
      if (subreddit && !FOOD_SUBREDDITS.has(subreddit)) return;

      const rawTitle = typeof obj.title === "string" ? obj.title.trim() : "";
      if (!rawTitle) return;

      const key = normalizeTitle(rawTitle);
      const selfText =
        typeof obj.selftext === "string" && obj.selftext.trim().length > 0
          ? obj.selftext.trim()
          : rawTitle;

      const baseScore = Number(obj.score ?? 0) || 0;
      const numComments = Number(obj.num_comments ?? 0) || 0;
      const social = Math.max(baseScore, 0) + Math.max(numComments * 3, 0);

      const existing = accumulators.get(key);
      if (!existing) {
        accumulators.set(key, {
          key,
          name: rawTitle,
          description: selfText.slice(0, 280),
          socialVolume: social,
          searchVolume: social * 2,
        });
      } else {
        existing.socialVolume += social;
        existing.searchVolume += social * 2;
      }

      const postId =
        typeof obj.id === "string" && obj.id.length > 0 ? obj.id : undefined;
      if (postId) {
        postIdToKey.set(postId, key);
      }
    },
    maxPosts
  );

  if (postIdToKey.size === 0 || accumulators.size === 0) {
    return [];
  }

  await streamZstJsonLines(
    commentsPath,
    (obj) => {
      if (!obj || typeof obj !== "object") return;

      const subreddit = String(obj.subreddit ?? "").toLowerCase();
      if (subreddit && !FOOD_SUBREDDITS.has(subreddit)) return;

      const linkId = String(obj.link_id ?? "");
      if (!linkId) return;

      const postId = linkId.startsWith("t3_") ? linkId.slice(3) : linkId;
      const key = postIdToKey.get(postId);
      if (!key) return;

      const acc = accumulators.get(key);
      if (!acc) return;

      const score = Number(obj.score ?? 0) || 0;
      const boost = 1 + Math.max(score, 0);
      acc.socialVolume += boost;
      acc.searchVolume += boost;
    },
    maxComments
  );

  const candidates = Array.from(accumulators.values());
  candidates.sort((a, b) => b.socialVolume - a.socialVolume);

  const topCandidates = candidates.slice(0, maxTrends);

  const insertPayloads: InsertTrend[] = topCandidates.map((c) => ({
    name: c.name,
    description: c.description,
    socialVolume: Math.round(c.socialVolume),
    searchVolume: Math.round(c.searchVolume),
    isEmerging: true,
    source: "Reddit",
    originalDish: null,
    indianAlternative: null,
  }));

  const created: Trend[] = [];
  for (const payload of insertPayloads) {
    const trend = await storage.createTrend(payload);
    created.push(trend);
  }

  return created;
}

