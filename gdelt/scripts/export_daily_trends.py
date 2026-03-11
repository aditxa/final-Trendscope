import argparse
import csv
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# Allow running as a standalone script (adds repo root so `import gdelt.src.*` works).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gdelt.src.trend_features import canonicalize_phrase, compute_trend_features, parse_bucket
from gdelt.src.trend_scoring import parse_weights, score_phrase
from gdelt.src.trend_storage import (
    connect_sqlite,
    ensure_phrase_counts_schema,
    fetch_articles_in_window,
    init_trend_tables,
    load_phrase_counts,
)
from gdelt.src.extract_phrases import (
    BRAND_LEXICON,
    DELIVERY_BRANDS,
    DISH_SUFFIXES,
    FOOD_BRANDS,
    FOOD_LEXICON,
    GENERIC_PHRASES,
    STOPWORDS,
    TOKEN_RE,
)


def _parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _daterange(start_day: date, end_day: date):
    day = start_day
    while day <= end_day:
        yield day
        day += timedelta(days=1)


def _percentile(samples: List[int], q: float) -> float:
    if not samples:
        return 0.0
    if q <= 0:
        return float(min(samples))
    if q >= 1:
        return float(max(samples))
    sorted_samples = sorted(samples)
    idx = int(round((len(sorted_samples) - 1) * q))
    return float(sorted_samples[idx])


def _title_tokens(title: str) -> List[str]:
    tokens = [token.lower() for token in TOKEN_RE.findall(title or "")]
    return [token for token in tokens if token and token not in STOPWORDS]


def _contains_phrase_tokens(title_tokens: List[str], phrase_tokens: List[str]) -> bool:
    if not phrase_tokens:
        return False
    if len(phrase_tokens) == 1:
        return phrase_tokens[0] in title_tokens
    if len(title_tokens) < len(phrase_tokens):
        return False
    n = len(phrase_tokens)
    for i in range(len(title_tokens) - n + 1):
        if title_tokens[i : i + n] == phrase_tokens:
            return True
    return False


def _is_food_phrase(phrase: str) -> bool:
    phrase = (phrase or "").strip().lower()
    if not phrase:
        return False
    if phrase in FOOD_LEXICON:
        return True
    if phrase in FOOD_BRANDS:
        return True
    tokens = [token for token in phrase.split() if token and token not in STOPWORDS]
    if not tokens:
        return False
    if any(token in FOOD_LEXICON for token in tokens):
        return True
    if any(token in FOOD_BRANDS for token in tokens):
        return True
    if any(token in DELIVERY_BRANDS for token in tokens):
        return any(token in FOOD_LEXICON for token in tokens) or (tokens[-1] in DISH_SUFFIXES)
    if len(tokens) >= 2:
        for n in (2, 3):
            if len(tokens) >= n:
                for i in range(len(tokens) - n + 1):
                    if " ".join(tokens[i : i + n]) in FOOD_LEXICON:
                        return True
    if tokens[-1] in DISH_SUFFIXES:
        return True
    return False


def _token_count(phrase: str) -> int:
    tokens = [token for token in (phrase or "").strip().lower().split() if token and token not in STOPWORDS]
    return len(tokens)


def _allow_unigram(phrase: str) -> bool:
    token = (phrase or "").strip().lower()
    if token in DELIVERY_BRANDS:
        return False
    return token in FOOD_LEXICON or token in FOOD_BRANDS or token in DISH_SUFFIXES


def export_daily_trends(args: argparse.Namespace) -> None:
    conn = connect_sqlite(args.db_path)
    init_trend_tables(conn)
    bucket_column = ensure_phrase_counts_schema(conn)

    rows = load_phrase_counts(conn, bucket_column)
    if not rows:
        raise SystemExit("phrase_counts is empty; run build_counts first")

    start_day = _parse_day(args.start_date)
    end_day = _parse_day(args.end_date)
    keep_start = datetime.combine(start_day, time.min, tzinfo=timezone.utc) - timedelta(days=int(args.baseline_days))
    keep_end = datetime.combine(end_day, time.min, tzinfo=timezone.utc) + timedelta(days=1)

    parsed_rows: List[Tuple[str, datetime, int]] = []
    variant_by_day: Dict[str, Dict[str, Dict[str, int]]] = {}

    for phrase, bucket_start, count in rows:
        dt = parse_bucket(bucket_start, str(args.phrase_counts_bucket))
        if not (keep_start <= dt < keep_end):
            continue
        canonical = canonicalize_phrase(phrase)
        if bool(args.food_only) and not _is_food_phrase(canonical):
            continue
        count_int = int(count)
        parsed_rows.append((canonical, dt, count_int))

        day_key = dt.date().isoformat()
        variant_by_day.setdefault(day_key, {}).setdefault(canonical, {})
        variant_by_day[day_key][canonical][phrase] = variant_by_day[day_key][canonical].get(phrase, 0) + count_int

    canonical_series = [(canonical, dt, count) for canonical, dt, count in parsed_rows]

    burst_percentile = float(args.burst_percentile)
    if burst_percentile > 1.0:
        burst_percentile /= 100.0
    burst_percentile = max(0.0, min(1.0, burst_percentile))

    weights = parse_weights(args.weights)

    os.makedirs(args.out_dir, exist_ok=True)

    total_days = 0
    total_written = 0

    window_days = int(getattr(args, "window_days", 1) or 1)
    if window_days < 1:
        raise SystemExit("--window-days must be >= 1")

    for day in _daterange(start_day, end_day):
        total_days += 1
        current_window_end = datetime.combine(day, time.min, tzinfo=timezone.utc) + timedelta(days=1)
        current_window_start = current_window_end - timedelta(days=window_days)
        baseline_end = current_window_start
        baseline_start = baseline_end - timedelta(days=int(args.baseline_days))
        window_delta = timedelta(days=window_days)

        features = compute_trend_features(
            canonical_series,
            current_window_start,
            current_window_end,
            baseline_start,
            baseline_end,
            window_delta,
        )

        trend_rows = []
        skipped_baseline = 0
        skipped_support = 0
        skipped_burst = 0
        skipped_score = 0
        skipped_domains = 0
        skipped_titles = 0
        skipped_syndication = 0

        window_start_iso = current_window_start.isoformat()
        window_end_iso = current_window_end.isoformat()

        day_articles = fetch_articles_in_window(conn, window_start_iso, window_end_iso)
        indexed_articles = []
        token_to_article_idxs: Dict[str, set[int]] = {}
        for idx, (title, url, seendate, domain) in enumerate(day_articles):
            tokens = _title_tokens(title)
            indexed_articles.append(
                {
                    "title": title,
                    "url": url,
                    "seendate": seendate,
                    "domain": domain,
                    "lower_title": (title or "").strip().lower(),
                    "tokens": tokens,
                }
            )
            for token in set(tokens):
                token_to_article_idxs.setdefault(token, set()).add(idx)

        for canonical, feature in features.items():
            if canonical in GENERIC_PHRASES:
                continue

            canonical_tokens = _token_count(canonical)
            if canonical_tokens < int(args.min_tokens):
                continue
            if canonical_tokens == 1 and bool(args.allow_unigram_food_brand) and not _allow_unigram(canonical):
                continue

            baseline_samples = feature.get("baseline_samples", [])
            if len(baseline_samples) < int(args.min_baseline_samples):
                skipped_baseline += 1
                continue

            if int(feature.get("current_count", 0)) < int(args.min_support):
                skipped_support += 1
                continue

            burst_cutoff = _percentile([int(x) for x in baseline_samples], burst_percentile)
            if float(feature.get("current_count", 0)) < burst_cutoff and float(feature.get("z_score", 0.0)) < float(args.z_threshold):
                skipped_burst += 1
                continue

            score, _extras = score_phrase(feature, weights, int(args.min_total))
            if float(score) < float(args.min_score):
                skipped_score += 1
                continue

            window_variant_totals: Dict[str, int] = {}
            for offset in range(window_days):
                key = (current_window_end.date() - timedelta(days=offset + 1)).isoformat()
                for variant, variant_count in variant_by_day.get(key, {}).get(canonical, {}).items():
                    window_variant_totals[variant] = window_variant_totals.get(variant, 0) + int(variant_count)
            phrase = max(window_variant_totals.items(), key=lambda item: item[1])[0] if window_variant_totals else canonical

            phrase_tokens = [t for t in (phrase or "").strip().lower().split() if t and t not in STOPWORDS]
            if not phrase_tokens:
                continue

            candidate_sets = [token_to_article_idxs.get(token, set()) for token in set(phrase_tokens)]
            if not candidate_sets:
                continue
            candidate_idxs = set.intersection(*sorted(candidate_sets, key=len))

            matched = []
            distinct_domains_set = set()
            distinct_titles_set = set()
            for idx in candidate_idxs:
                art = indexed_articles[idx]
                if _contains_phrase_tokens(art["tokens"], phrase_tokens):
                    matched.append(art)
                    distinct_domains_set.add(art["domain"])
                    distinct_titles_set.add(art["lower_title"])

            if len(distinct_domains_set) < int(args.min_domains):
                skipped_domains += 1
                continue
            if len(distinct_titles_set) < int(args.min_titles):
                skipped_titles += 1
                continue

            max_domains_per_title = int(getattr(args, "max_domains_per_title", 0) or 0)
            if max_domains_per_title > 0 and len(distinct_titles_set) > 0:
                if len(distinct_domains_set) > (max_domains_per_title * len(distinct_titles_set)):
                    skipped_syndication += 1
                    continue

            matched.sort(key=lambda a: a.get("seendate", ""), reverse=True)
            matched = matched[: int(args.evidence_n)]
            sample_titles = [a["title"] for a in matched]
            sample_urls = [a["url"] for a in matched]

            trend_rows.append(
                (
                    phrase,
                    canonical,
                    float(score),
                    int(feature.get("current_count", 0)),
                    float(feature.get("baseline_mean", 0.0)),
                    float(feature.get("baseline_std", 0.0)),
                    float(feature.get("z_score", 0.0)),
                    float(feature.get("growth_ratio", 0.0)),
                    float(feature.get("velocity", 0.0)),
                    float(feature.get("novelty_days", 0.0)),
                    json.dumps(sample_titles),
                    json.dumps(sample_urls),
                )
            )

        trend_rows.sort(key=lambda item: item[2], reverse=True)
        trend_rows = trend_rows[: int(args.top_k)]

        out_path = os.path.join(args.out_dir, f"{day.isoformat()}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "phrase",
                    "canonical_phrase",
                    "trend_score",
                    "current_count",
                    "baseline_mean",
                    "baseline_std",
                    "z_score",
                    "growth_ratio",
                    "velocity",
                    "novelty_days",
                    "sample_titles",
                    "sample_urls",
                ]
            )
            for row in trend_rows:
                writer.writerow(row)

        total_written += 1
        logging.info(
            "%s: wrote %s rows (skipped baseline=%s support=%s burst=%s score=%s domains=%s titles=%s synd=%s)",
            day.isoformat(),
            len(trend_rows),
            skipped_baseline,
            skipped_support,
            skipped_burst,
            skipped_score,
            skipped_domains,
            skipped_titles,
            skipped_syndication,
        )

    conn.close()
    logging.info("Done. Wrote %s daily CSVs for %s days into %s", total_written, total_days, args.out_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export per-day trend CSVs from phrase_counts")
    parser.add_argument("--db-path", default="data/gdelt_food.db")
    parser.add_argument("--out-dir", default="data/daily_trends")

    parser.add_argument(
        "--phrase-counts-bucket",
        choices=["daily", "hourly"],
        default="daily",
        help="How phrase_counts.bucket_start (or legacy date) is stored (default: daily)",
    )
    parser.add_argument(
        "--food-only",
        action="store_true",
        help="Filter detected phrases to food-related keywords",
    )

    parser.add_argument(
        "--min-tokens",
        type=int,
        default=1,
        help="Minimum non-stopword token count for exported phrases (default: 1)",
    )
    parser.add_argument(
        "--allow-unigram-food-brand",
        action="store_true",
        help="Allow unigram phrases, but only if they look like food/brand terms",
    )

    parser.add_argument("--start-date", default="2025-12-01", help="YYYY-MM-DD")
    parser.add_argument("--end-date", default="2025-12-30", help="YYYY-MM-DD")

    parser.add_argument(
        "--window-days",
        type=int,
        default=1,
        help="Rolling window size in days for scoring/evidence (default: 1)",
    )

    parser.add_argument("--baseline-days", type=int, default=28)
    parser.add_argument("--top-k", type=int, default=50)

    parser.add_argument("--min-total", type=int, default=5)
    parser.add_argument("--min-score", type=float, default=6.0)
    parser.add_argument("--min-support", type=int, default=3)
    parser.add_argument("--min-baseline-samples", type=int, default=7)
    parser.add_argument("--min-domains", type=int, default=2)
    parser.add_argument(
        "--min-titles",
        type=int,
        default=1,
        help="Minimum distinct matching titles in the window (default: 1)",
    )
    parser.add_argument(
        "--max-domains-per-title",
        type=int,
        default=6,
        help="Suppress syndicated repost storms (default: 6)",
    )

    parser.add_argument("--weights", default="z=1.0,g=0.8,v=0.6,n=0.4")
    parser.add_argument("--evidence-n", type=int, default=5)
    parser.add_argument("--z-threshold", type=float, default=2.0)
    parser.add_argument("--burst-percentile", type=float, default=0.95)

    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    export_daily_trends(args)


if __name__ == "__main__":
    main()
