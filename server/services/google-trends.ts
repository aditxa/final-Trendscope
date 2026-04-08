import googleTrends from "google-trends-api";

export interface TrendPoint {
  date: string;
  value: number;
}

export interface TrendInterestResult {
  name: string;
  points: TrendPoint[];
}

/**
 * Fetch real Google Trends "interest over time" data for a keyword.
 * Uses the last 3 months, geo=IN (India).
 */
export async function fetchGoogleTrendsInterest(
  keyword: string
): Promise<TrendInterestResult> {
  const endTime = new Date();
  const startTime = new Date();
  startTime.setMonth(startTime.getMonth() - 3);

  const rawJson = await googleTrends.interestOverTime({
    keyword,
    startTime,
    endTime,
    geo: "IN",
  });

  const parsed = JSON.parse(rawJson);
  const timelineData = parsed?.default?.timelineData;

  if (!timelineData || timelineData.length === 0) {
    throw new Error(`No Google Trends data returned for "${keyword}"`);
  }

  const points: TrendPoint[] = timelineData.map(
    (entry: { formattedAxisTime: string; value: number[] }) => ({
      date: entry.formattedAxisTime,
      value: entry.value[0] ?? 0,
    })
  );

  return { name: keyword, points };
}
