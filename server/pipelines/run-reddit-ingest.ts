import { ingestRedditTrendsFromFiles } from "./reddit";

async function main() {
  try {
    const trends = await ingestRedditTrendsFromFiles();
    console.log(
      `Reddit ingestion completed. Inserted ${trends.length} new trend(s) into the database.`
    );
    process.exit(0);
  } catch (err) {
    console.error("Reddit ingestion failed:", err);
    process.exit(1);
  }
}

main();

