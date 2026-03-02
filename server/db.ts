import "dotenv/config";
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import * as schema from "@shared/schema";

const { Pool } = pg;

/**
 * In a full Postgres setup, DATABASE_URL should be defined and we construct
 * a real Pool + Drizzle client. If it's missing, we gracefully degrade to
 * `null` so higher layers can fall back to file-based storage.
 */
export const pool =
  process.env.DATABASE_URL != null
    ? new Pool({ connectionString: process.env.DATABASE_URL })
    : null;

// `db` is typed as `any` so callers can compile even when it's null at runtime.
export const db: any =
  pool != null ? drizzle(pool, { schema }) : null;
