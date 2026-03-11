import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import json
import uuid

from .gdelt_client import GdeltConfig, adaptive_fetch, generate_run_once_window, to_gdelt_timestamp
from .storage import Storage, StorageConfig
from .extract_phrases import compute_total_counts, export_candidates_to_csv, extract_candidates, flatten_counts
from .extract_phrases import DISH_SUFFIXES, FOOD_LEXICON, TOKEN_RE
from .trend_counts import build_phrase_counts
from .trend_features import canonicalize_phrase, compute_trend_features, parse_bucket
from .trend_scoring import parse_weights, score_phrase
from .trend_storage import (
    connect_sqlite,
    ensure_phrase_counts_schema,
    init_trend_tables,
    insert_trend_run,
    insert_trends,
    latest_run_id,
    load_phrase_counts,
    fetch_sample_articles,
    count_distinct_domains,
    load_trends,
)

DEFAULT_QUERY = (
    "(near10:\"viral recipe\" OR near10:\"tiktok recipe\" OR \"food trend\" OR "
    "near10:\"trending recipe\" OR near10:\"viral food\")"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GDELT food trend ingestion pipeline")
    parser.add_argument("--db-path", default="data/gdelt_food.db", help="SQLite DB path")
    parser.add_argument("--language", default="English", help="GDELT language filter (e.g., English)")
    parser.add_argument("--source-country", default=None, help="GDELT sourcecountry filter")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="GDELT query")
    parser.add_argument("--maxrecords", type=int, default=250, help="GDELT maxrecords")
    parser.add_argument("--min-window-seconds", type=int, default=900, help="Minimum window size in seconds")
    parser.add_argument("--rate-limit-seconds", type=float, default=5.0, help="Rate limit between requests")
    parser.add_argument("--retries", type=int, default=5, help="Max retries")
    parser.add_argument("--timeout", type=int, default=20, help="Request timeout seconds")
    parser.add_argument("--use-postgres", action="store_true", help="Use Postgres instead of SQLite")
    parser.add_argument("--postgres-dsn", default=None, help="Postgres DSN string")
    parser.add_argument(
        "--title-filter",
        choices=["none", "food_trend", "food_context"],
        default="food_trend",
        help="Post-filter articles by title (default: food_trend)",
    )

    english_group = parser.add_mutually_exclusive_group()
    english_group.add_argument(
        "--english-only",
        action="store_true",
        default=True,
        help="Filter ingestion to English-only (default)",
    )
    english_group.add_argument(
        "--allow-non-english",
        dest="english_only",
        action="store_false",
        help="Do not filter by English at ingestion",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_once = subparsers.add_parser("run_once", help="Ingest last N minutes")
    run_once.add_argument("--minutes", type=int, default=60)
    run_once.add_argument("--overlap-minutes", type=int, default=5)

    backfill = subparsers.add_parser("backfill", help="Backfill between dates inclusive")
    backfill.add_argument(
        "--start-date",
        "--start",
        dest="start_date",
        default=None,
        help="YYYY-MM-DD (optional; if omitted uses --days)",
    )
    backfill.add_argument(
        "--end-date",
        "--end",
        dest="end_date",
        default=None,
        help="YYYY-MM-DD (optional; defaults to today when --start-date is provided)",
    )
    backfill.add_argument(
        "--days",
        type=int,
        default=28,
        help="Backfill last N full days when start/end are omitted (default 28)",
    )

    extract = subparsers.add_parser("extract", help="Extract food phrase candidates")
    extract.add_argument("--csv-path", default="data/phrase_candidates.csv")

    build_counts = subparsers.add_parser("build_counts", help="Build phrase_counts from articles")
    build_counts.add_argument("--bucket", choices=["daily", "hourly"], default="daily")
    build_counts.add_argument(
        "--method",
        choices=["ngrams", "spacy"],
        default="spacy",
        help="Phrase extraction method for phrase_counts (default: spacy)",
    )
    build_counts.add_argument("--food-only", action="store_true", help="Filter phrases to food-related keywords")
    build_counts.add_argument(
        "--include-unigrams",
        action="store_true",
        help="Also include single-word phrases (useful when n-grams are too sparse)",
    )
    build_counts.add_argument("--start-date", "--start", dest="start_date", default=None, help="YYYY-MM-DD")
    build_counts.add_argument("--end-date", "--end", dest="end_date", default=None, help="YYYY-MM-DD")

    detect = subparsers.add_parser("detect", help="Detect trending phrases")
    detect.add_argument("--bucket", choices=["daily", "hourly"], default="daily")
    detect.add_argument("--current-window-hours", type=int, default=24)
    detect.add_argument("--baseline-days", type=int, default=14)
    detect.add_argument("--top-k", type=int, default=50)
    detect.add_argument("--min-total", type=int, default=5)
    detect.add_argument("--min-support", type=int, default=3)
    detect.add_argument("--min-baseline-samples", type=int, default=7)
    detect.add_argument("--min-domains", type=int, default=2)
    detect.add_argument(
        "--min-score",
        type=float,
        default=6.0,
        help="Minimum trend score to keep (default 6.0)",
    )
    detect.add_argument("--weights", default="z=1.0,g=0.8,v=0.6,n=0.4")
    detect.add_argument("--evidence-n", type=int, default=5)
    detect.add_argument("--z-threshold", type=float, default=2.0)
    detect.add_argument("--burst-percentile", type=float, default=0.95)

    export = subparsers.add_parser("export", help="Export trends to CSV/JSON")
    export.add_argument("--run-id", default=None)
    export.add_argument("--top-k", type=int, default=50)
    export.add_argument("--format", choices=["csv", "json"], default="csv")
    export.add_argument("--output", default="data/trends_export.csv")

    export_articles = subparsers.add_parser("export_articles", help="Export ingested articles to CSV/JSON")
    export_articles.add_argument("--format", choices=["csv", "json"], default="csv")
    export_articles.add_argument("--output", default="data/articles_export.csv")
    export_articles.add_argument("--start-date", "--start", dest="start_date", default=None, help="YYYY-MM-DD")
    export_articles.add_argument("--end-date", "--end", dest="end_date", default=None, help="YYYY-MM-DD")
    export_articles.add_argument(
        "--all",
        action="store_true",
        help="Export all ingested articles (ignores --start-date/--end-date/--days)",
    )
    export_articles.add_argument(
        "--days",
        type=int,
        default=28,
        help="Export last N full days when start/end are omitted (default 28)",
    )
    export_articles.add_argument("--limit", type=int, default=None, help="Optional max rows")

    purge_articles = subparsers.add_parser("purge_articles", help="Delete ingested articles in a date range")
    purge_articles.add_argument(
        "--start-date",
        "--start",
        dest="start_date",
        default=None,
        help="YYYY-MM-DD (optional; if omitted uses --days)",
    )
    purge_articles.add_argument(
        "--end-date",
        "--end",
        dest="end_date",
        default=None,
        help="YYYY-MM-DD (optional; defaults to today when --start-date is provided)",
    )
    purge_articles.add_argument(
        "--days",
        type=int,
        default=28,
        help="Purge last N full days when start/end are omitted (default 28)",
    )
    purge_articles.add_argument(
        "--keep-derived",
        action="store_true",
        help="Do not clear derived tables (phrase_counts/trends/trend_runs/checkpoints)",
    )

    return parser.parse_args()


def run_purge_articles(args: argparse.Namespace) -> None:
    storage_config = StorageConfig(
        sqlite_path=args.db_path,
        use_postgres=args.use_postgres,
        postgres_dsn=args.postgres_dsn,
    )
    storage = Storage(storage_config)
    storage.init_db()

    if args.end_date and not args.start_date:
        raise SystemExit("purge_articles: --end-date requires --start-date")

    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.end_date:
            end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            # End defaults to today; include today up to now.
            end_dt = datetime.now(tz=timezone.utc)

        # Use an exclusive end bound.
        if args.end_date:
            end_exclusive = end_dt + timedelta(days=1)
        else:
            end_exclusive = end_dt

        start_day_iso = start_dt.date().isoformat()
        end_label = end_dt.date().isoformat() if args.end_date else "today"
    else:
        start_dt, end_dt, start_day_iso, end_day_iso = _resolve_date_range_for_days(int(args.days or 28))
        end_exclusive = end_dt + timedelta(days=1)
        end_label = end_day_iso

    start_iso = start_dt.isoformat()
    end_iso = end_exclusive.isoformat()
    start_gdelt = to_gdelt_timestamp(start_dt)
    end_gdelt = to_gdelt_timestamp(end_exclusive)

    placeholder = "?" if storage.backend == "sqlite" else "%s"
    delete_sql = (
        "DELETE FROM articles "
        f"WHERE (seendate >= {placeholder} AND seendate < {placeholder}) "
        f"   OR (seendate >= {placeholder} AND seendate < {placeholder})"
    )
    params = (start_iso, end_iso, start_gdelt, end_gdelt)
    cur = storage.conn.cursor()
    cur.execute(delete_sql, params)
    deleted_articles = cur.rowcount
    storage.conn.commit()

    if not getattr(args, "keep_derived", False):
        # These are derived from articles and will otherwise be stale.
        for table in ("phrase_counts", "trends", "trend_runs", "checkpoints"):
            try:
                cur.execute(f"DELETE FROM {table}")
            except Exception as exc:
                logging.warning("Could not clear table %s: %s", table, exc)
        storage.conn.commit()

    logging.info(
        "Purged %s articles from %s to %s (keep_derived=%s)",
        deleted_articles,
        start_day_iso,
        end_label,
        bool(getattr(args, "keep_derived", False)),
    )
    storage.close()


def _resolve_date_range_for_days(days: int) -> tuple[datetime, datetime, str, str]:
    """Returns (start_dt, end_dt, start_day_iso, end_day_iso) for last N full days (UTC).

    end_dt is the start of the end_day (midnight UTC); callers often want end_dt + 1 day.
    """
    if days <= 0:
        raise SystemExit("--days must be >= 1")
    end_day = (datetime.now(tz=timezone.utc) - timedelta(days=1)).date()
    start_day = end_day - timedelta(days=days - 1)
    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
    return start_dt, end_dt, start_day.isoformat(), end_day.isoformat()


def build_gdelt_config(args: argparse.Namespace) -> GdeltConfig:
    language = args.language.strip()
    return GdeltConfig(
        query=args.query,
        language=language,
        source_country=args.source_country,
        max_records=args.maxrecords,
        timeout_seconds=args.timeout,
        max_retries=args.retries,
        rate_limit_seconds=args.rate_limit_seconds,
        min_window_seconds=args.min_window_seconds,
    )


def run_ingestion(start_dt: datetime, end_dt: datetime, args: argparse.Namespace) -> int:
    gdelt_config = build_gdelt_config(args)
    storage_config = StorageConfig(
        sqlite_path=args.db_path,
        use_postgres=args.use_postgres,
        postgres_dsn=args.postgres_dsn,
    )

    storage = Storage(storage_config)
    storage.init_db()

    inserted_total = 0
    skipped_language = 0
    skipped_missing_title = 0
    skipped_title_filter = 0
    filtered_articles = []
    for article in adaptive_fetch(start_dt, end_dt, gdelt_config):
        title = article.get("title")
        if not title:
            skipped_missing_title += 1
            continue
        if getattr(args, "english_only", True) and not _is_english_article(article, title):
            skipped_language += 1
            continue
        title_filter = getattr(args, "title_filter", "food_trend")
        if title_filter == "food_trend" and not _is_food_trend_title(title):
            skipped_title_filter += 1
            continue
        if title_filter == "food_context" and not _is_food_context_title(title):
            skipped_title_filter += 1
            continue
        filtered_articles.append(article)

    inserted_total += storage.insert_articles(filtered_articles, gdelt_config.query)
    storage.update_checkpoint(to_gdelt_timestamp(end_dt))

    logging.info(
        "Inserted %s articles (skipped %s non-English, %s missing title, %s title-filter)",
        inserted_total,
        skipped_language,
        skipped_missing_title,
        skipped_title_filter,
    )
    storage.close()
    return inserted_total


_RECIPE_HINTS = {
    "recipe",
    "recipes",
    "cook",
    "cooking",
    "bake",
    "baked",
    "air fryer",
    "instant pot",
    "slow cooker",
    "dinner",
    "lunch",
    "breakfast",
    "dessert",
    "snack",
    "ingredients",
}

_TREND_HINTS = {
    "viral",
    "trending",
    "trend",
    "tiktok",
    "instagram",
}

_PROMO_HINTS = {
    "kit",
    "free",
    "limited time",
    "limited-time",
    "menu",
    "new",
    "drops",
    "drop",
    "launch",
    "launches",
    "offering",
    "offer",
    "collab",
    "collaboration",
}

_FOOD_BRANDS = {
    "mcdonald",
    "mcdonalds",
    "mcnugget",
    "mcnuggets",
    "starbucks",
    "dunkin",
    "chipotle",
    "taco bell",
    "kfc",
    "subway",
    "wendy",
    "burger king",
    "domino",
    "dominos",
    "pizza hut",
}

# Cuisine/demonym tokens can appear in non-food headlines (e.g., "Viral Chinese Trump...").
# Treat these as a weak food signal unless the title also contains clear food context.
_WEAK_FOOD_TOKENS = {
    "chinese",
    "japanese",
    "korean",
    "thai",
    "vietnamese",
    "indian",
    "mexican",
    "italian",
    "french",
    "greek",
    "turkish",
    "mediterranean",
    "middle",
    "eastern",
}

_FOOD_CONTEXT_HINTS = {
    "food",
    "recipe",
    "recipes",
    "cook",
    "cooking",
    "bake",
    "baked",
    "ingredients",
    "dish",
    "snack",
    "dessert",
    "drink",
    "restaurant",
    "menu",
}

_NEWS_EXCLUDE_HINTS = {
    "hospital",
    "hospitalised",
    "hospitalized",
    "fda",
    "cdc",
    "recall",
    "outbreak",
    "food poisoning",
    "poisoning",
    "urges consumers",
    "report suspicious",
    "death",
    "dies",
    "killed",
    "missing",
}


def _is_food_trend_title(title: str) -> bool:
    """Heuristic filter to keep food trend/recipe content and drop incidental 'food' news.

    This intentionally prefers precision over recall.
    """
    text = (title or "").strip().lower()
    if not text:
        return False

    if any(hint in text for hint in _NEWS_EXCLUDE_HINTS):
        return False

    tokens = [t.lower() for t in TOKEN_RE.findall(text)]
    if not tokens:
        return False

    # Food signal: explicit food lexicon term, dish-like suffix, or known food brand.
    # Avoid demonym-only matches like "Viral Chinese Trump...".
    brand_signal = any(b in text for b in _FOOD_BRANDS)
    food_tokens = {t for t in tokens if t in FOOD_LEXICON}
    strong_food_tokens = {t for t in food_tokens if t not in _WEAK_FOOD_TOKENS}
    context_signal = any(h in text for h in _FOOD_CONTEXT_HINTS)

    food_signal = (
        brand_signal
        or (tokens[-1] in DISH_SUFFIXES)
        or bool(strong_food_tokens)
        or (bool(food_tokens) and context_signal)
    )

    # Intent signal: recipe/how-to/trend language.
    intent_signal = (
        any(h in text for h in _RECIPE_HINTS)
        or any(h in text for h in _TREND_HINTS)
        or any(h in text for h in _PROMO_HINTS)
    )

    # Allow intent-led matches even if lexicon misses the dish name.
    if intent_signal and food_signal:
        return True

    # Otherwise require clear food signal.
    return food_signal and intent_signal


def _is_food_context_title(title: str) -> bool:
    """Heuristic filter to keep food-related content without requiring 'trend' language.

    This is less strict than _is_food_trend_title and is a good default for backfills
    where you want broader food coverage but still want to avoid totally unrelated news.
    """
    text = (title or "").strip().lower()
    if not text:
        return False

    if any(hint in text for hint in _NEWS_EXCLUDE_HINTS):
        return False

    tokens = [t.lower() for t in TOKEN_RE.findall(text)]
    if not tokens:
        return False

    brand_signal = any(b in text for b in _FOOD_BRANDS)
    food_tokens = {t for t in tokens if t in FOOD_LEXICON}
    strong_food_tokens = {t for t in food_tokens if t not in _WEAK_FOOD_TOKENS}
    context_signal = any(h in text for h in _FOOD_CONTEXT_HINTS)

    return (
        brand_signal
        or (tokens[-1] in DISH_SUFFIXES)
        or bool(strong_food_tokens)
        or (bool(food_tokens) and context_signal)
    )


def run_extract(args: argparse.Namespace) -> None:
    storage_config = StorageConfig(
        sqlite_path=args.db_path,
        use_postgres=args.use_postgres,
        postgres_dsn=args.postgres_dsn,
    )
    storage = Storage(storage_config)
    storage.init_db()

    titles = storage.load_titles()
    counts_by_date = extract_candidates(titles)
    totals = compute_total_counts(counts_by_date)

    rows = flatten_counts(counts_by_date)
    storage.upsert_phrase_counts(rows)

    export_candidates_to_csv(counts_by_date, totals, args.csv_path)

    logging.info("Exported phrase candidates to %s", args.csv_path)
    storage.close()


def run_build_counts(args: argparse.Namespace) -> None:
    conn = connect_sqlite(args.db_path)
    init_trend_tables(conn)

    start_dt: datetime | None = None
    end_dt: datetime | None = None
    if getattr(args, "start_date", None):
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if getattr(args, "end_date", None):
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)

    build_phrase_counts(
        conn,
        bucket=args.bucket,
        food_only=args.food_only,
        include_unigrams=bool(getattr(args, "include_unigrams", False)),
        method=str(getattr(args, "method", "spacy")),
        start_dt=start_dt,
        end_dt=end_dt,
    )
    conn.close()


def _generate_run_id() -> str:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"{timestamp}_{suffix}"


def run_detect(args: argparse.Namespace) -> None:
    conn = connect_sqlite(args.db_path)
    init_trend_tables(conn)
    _log_db_time_range(conn)
    bucket_column = ensure_phrase_counts_schema(conn)
    rows = load_phrase_counts(conn, bucket_column)
    if not rows:
        logging.warning("phrase_counts is empty; run build_counts first")
        conn.close()
        return

    burst_percentile = float(getattr(args, "burst_percentile", 0.95))
    # Accept either [0,1] (0.95) or [0,100] (95).
    if burst_percentile > 1.0:
        burst_percentile = burst_percentile / 100.0
    burst_percentile = max(0.0, min(1.0, burst_percentile))

    bucket = args.bucket
    parsed_rows = []
    variant_totals = {}

    for phrase, bucket_start, count in rows:
        dt = parse_bucket(bucket_start, bucket)
        canonical = canonicalize_phrase(phrase)
        parsed_rows.append((canonical, dt, count, phrase))
        variant_totals.setdefault(canonical, {})
        variant_totals[canonical][phrase] = variant_totals[canonical].get(phrase, 0) + count

    canonical_series = [(canonical, dt, count) for canonical, dt, count, _ in parsed_rows]
    max_dt = max(dt for _, dt, _ in canonical_series)
    window_delta = timedelta(hours=args.current_window_hours)
    current_window_end = max_dt + (timedelta(hours=1) if bucket == "hourly" else timedelta(days=1))
    current_window_start = current_window_end - window_delta
    baseline_end = current_window_start
    baseline_start = baseline_end - timedelta(days=args.baseline_days)

    baseline_bucket_available = _count_baseline_buckets(conn, bucket_column, bucket, baseline_start, baseline_end)
    logging.info(
        "Baseline buckets available=%s for baseline_days=%s (bucket=%s)",
        baseline_bucket_available,
        args.baseline_days,
        bucket,
    )

    features = compute_trend_features(
        canonical_series,
        current_window_start,
        current_window_end,
        baseline_start,
        baseline_end,
        window_delta,
    )

    weights = parse_weights(args.weights)
    run_id = _generate_run_id()
    insert_trend_run(
        conn,
        run_id,
        {
            "bucket": bucket,
            "current_window_hours": args.current_window_hours,
            "baseline_days": args.baseline_days,
            "min_total": args.min_total,
            "min_score": float(getattr(args, "min_score", 0.0)),
            "weights": weights,
        },
    )

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

    skipped_baseline = 0
    skipped_support = 0
    skipped_burst = 0
    skipped_score = 0
    skipped_domains = 0
    evidence_success = 0
    trend_rows = []
    for canonical, feature in features.items():
        baseline_total = feature["baseline_mean"] * max(1, args.baseline_days)
        if len(feature.get("baseline_samples", [])) < args.min_baseline_samples:
            skipped_baseline += 1
            continue

        if feature["current_count"] < args.min_support:
            skipped_support += 1
            continue

        burst_cutoff = _percentile(feature.get("baseline_samples", []), burst_percentile)
        if feature["current_count"] < burst_cutoff and feature["z_score"] < args.z_threshold:
            skipped_burst += 1
            continue

        score, extras = score_phrase(feature, weights, args.min_total)

        if float(score) < float(getattr(args, "min_score", 0.0)):
            skipped_score += 1
            continue

        variants = variant_totals.get(canonical, {})
        if variants:
            phrase = max(variants.items(), key=lambda item: item[1])[0]
        else:
            phrase = canonical

        window_start = current_window_start.isoformat()
        window_end = current_window_end.isoformat()
        distinct_domains = count_distinct_domains(conn, phrase, window_start, window_end)
        if distinct_domains < args.min_domains:
            skipped_domains += 1
            continue

        sample_articles = fetch_sample_articles(
            conn,
            phrase,
            window_start,
            window_end,
            limit=args.evidence_n,
        )
        if sample_articles:
            evidence_success += 1
        sample_titles = [title for title, _, _, _ in sample_articles]
        sample_urls = [url for _, url, _, _ in sample_articles]

        trend_rows.append(
            (
                run_id,
                phrase,
                canonical,
                float(score),
                int(feature["current_count"]),
                float(feature["baseline_mean"]),
                float(feature["baseline_std"]),
                float(feature["z_score"]),
                float(feature["growth_ratio"]),
                float(feature["velocity"]),
                float(feature["novelty_days"]),
                current_window_start.isoformat(),
                current_window_end.isoformat(),
                baseline_start.isoformat(),
                baseline_end.isoformat(),
                json.dumps(sample_titles),
                json.dumps(sample_urls),
                datetime.now(tz=timezone.utc).isoformat(),
            )
        )

    trend_rows.sort(key=lambda item: item[3], reverse=True)
    insert_trends(conn, trend_rows[: args.top_k])
    logging.info("Stored %s trends for run %s", min(args.top_k, len(trend_rows)), run_id)
    logging.info(
        "Skipped %s baseline-insufficient, %s min-support, %s burst-gate, %s score-threshold, %s domain-gate",
        skipped_baseline,
        skipped_support,
        skipped_burst,
        skipped_score,
        skipped_domains,
    )
    logging.info("Evidence present for %s trends", evidence_success)
    conn.close()


def _is_english_article(article: Dict, title: str) -> bool:
    language = (article.get("language") or "").strip().lower()
    if language and language not in {"english", "en", "eng"}:
        return False

    ascii_chars = sum(1 for ch in title if ord(ch) < 128)
    ratio = ascii_chars / max(1, len(title))
    return ratio >= 0.85


def _log_db_time_range(conn) -> None:
    try:
        cur = conn.cursor()
        cur.execute("SELECT MIN(seendate), MAX(seendate), COUNT(*) FROM articles")
        row = cur.fetchone()
        if not row:
            return
        logging.info("DB articles=%s seendate_min=%s seendate_max=%s", row[2], row[0], row[1])
    except Exception as exc:
        logging.warning("Could not read DB time range: %s", exc)


def _bucket_key(dt: datetime, bucket: str) -> str:
    if bucket == "hourly":
        return dt.replace(minute=0, second=0, microsecond=0).isoformat()
    return dt.date().isoformat()


def _count_baseline_buckets(conn, bucket_column: str, bucket: str, baseline_start: datetime, baseline_end: datetime) -> int:
    start_key = _bucket_key(baseline_start, bucket)
    end_key = _bucket_key(baseline_end, bucket)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(DISTINCT {bucket_column}) FROM phrase_counts WHERE {bucket_column} >= ? AND {bucket_column} < ?",
            (start_key, end_key),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:
        logging.warning("Could not count baseline buckets: %s", exc)
        return 0


def run_export(args: argparse.Namespace) -> None:
    conn = connect_sqlite(args.db_path)
    init_trend_tables(conn)
    run_id = args.run_id or latest_run_id(conn)
    if not run_id:
        logging.warning("No trend runs found")
        conn.close()
        return

    rows = load_trends(conn, run_id, args.top_k)
    conn.close()

    if args.format == "json":
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(
                [
                    {
                        "phrase": row[0],
                        "canonical_phrase": row[1],
                        "trend_score": row[2],
                        "current_count": row[3],
                        "baseline_mean": row[4],
                        "baseline_std": row[5],
                        "z_score": row[6],
                        "growth_ratio": row[7],
                        "velocity": row[8],
                        "novelty_days": row[9],
                        "sample_titles": json.loads(row[10]),
                        "sample_urls": json.loads(row[11]),
                    }
                    for row in rows
                ],
                handle,
                indent=2,
            )
    else:
        import csv

        with open(args.output, "w", newline="", encoding="utf-8") as handle:
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
            for row in rows:
                writer.writerow(row)

    logging.info("Exported trends to %s", args.output)


def run_export_articles(args: argparse.Namespace) -> None:
    storage_config = StorageConfig(
        sqlite_path=args.db_path,
        use_postgres=args.use_postgres,
        postgres_dsn=args.postgres_dsn,
    )
    storage = Storage(storage_config)
    storage.init_db()

    if args.all:
        args.start_date = None
        args.end_date = None

    if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
        raise SystemExit("export_articles: provide both --start-date and --end-date, or neither")

    if args.all or (args.days is not None and int(args.days) == 0):
        start_day_iso = "ALL"
        end_day_iso = "ALL"
        if storage.backend == "sqlite":
            placeholder = "?"
        else:
            placeholder = "%s"

        sql = (
            "SELECT url, title, seendate, domain, language, sourcecountry, snippet, socialimage, inserted_at "
            "FROM articles "
            "ORDER BY seendate DESC"
        )
        params: list[object] = []
    elif args.start_date and args.end_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_day_iso = start_dt.date().isoformat()
        end_day_iso = end_dt.date().isoformat()
    else:
        start_dt, end_dt, start_day_iso, end_day_iso = _resolve_date_range_for_days(int(args.days or 28))

        # Make end exclusive by adding one day.
        end_exclusive = end_dt + timedelta(days=1)
        start_iso = start_dt.isoformat()
        end_iso = end_exclusive.isoformat()
        start_gdelt = to_gdelt_timestamp(start_dt)
        end_gdelt = to_gdelt_timestamp(end_exclusive)

        if storage.backend == "sqlite":
            placeholder = "?"
        else:
            placeholder = "%s"

        sql = (
            "SELECT url, title, seendate, domain, language, sourcecountry, snippet, socialimage, inserted_at "
            "FROM articles "
            f"WHERE (seendate >= {placeholder} AND seendate < {placeholder}) "
            f"OR (seendate >= {placeholder} AND seendate < {placeholder}) "
            "ORDER BY seendate DESC"
        )
        params = [start_iso, end_iso, start_gdelt, end_gdelt]

    if args.limit is not None:
        limit = int(args.limit)
        if limit > 0:
            sql += f" LIMIT {limit}"

    cur = storage.conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()

    if args.format == "json":
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(
                [
                    {
                        "url": r[0],
                        "title": r[1],
                        "seendate": r[2],
                        "domain": r[3],
                        "language": r[4],
                        "sourcecountry": r[5],
                        "snippet": r[6],
                        "socialimage": r[7],
                        "inserted_at": r[8],
                    }
                    for r in rows
                ],
                handle,
                indent=2,
            )
    else:
        import csv

        with open(args.output, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "url",
                    "title",
                    "seendate",
                    "domain",
                    "language",
                    "sourcecountry",
                    "snippet",
                    "socialimage",
                    "inserted_at",
                ]
            )
            writer.writerows(rows)

    logging.info("Exported %s articles (%s to %s) to %s", len(rows), start_day_iso, end_day_iso, args.output)
    storage.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    if args.command == "run_once":
        start_dt, end_dt = generate_run_once_window(args.minutes, args.overlap_minutes)
        run_ingestion(start_dt, end_dt, args)
        return

    if args.command == "backfill":
        if args.end_date and not args.start_date:
            raise SystemExit("backfill: --end-date requires --start-date")

        if args.start_date:
            start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if args.end_date:
                end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            else:
                # If only a start date is provided, backfill through today.
                today_utc = datetime.now(tz=timezone.utc).date()
                end_dt = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
        else:
            days = int(getattr(args, "days", 28) or 28)
            if days <= 0:
                raise SystemExit("backfill: --days must be >= 1")

            # Default behavior: backfill the last N full days up through yesterday (UTC)
            # to avoid querying into the future.
            end_day = (datetime.now(tz=timezone.utc) - timedelta(days=1)).date()
            start_day = end_day - timedelta(days=days - 1)
            start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(end_day, datetime.min.time(), tzinfo=timezone.utc)
            logging.info("Backfill last %s days: %s to %s", days, start_day.isoformat(), end_day.isoformat())

        current = start_dt
        now_utc = datetime.now(tz=timezone.utc)
        today_utc = now_utc.date()
        while current <= end_dt:
            day_start = current
            day_end = min(current + timedelta(days=1), end_dt + timedelta(days=1))
            # If the user backfills through today, don't query into the future.
            if end_dt.date() == today_utc and day_start.date() == today_utc:
                day_end = min(day_end, now_utc)
            logging.info("Backfill day %s", day_start.date())
            run_ingestion(day_start, day_end, args)
            current += timedelta(days=1)
        return

    if args.command == "purge_articles":
        run_purge_articles(args)
        return

    if args.command == "extract":
        run_extract(args)
        return

    if args.command == "build_counts":
        run_build_counts(args)
        return

    if args.command == "detect":
        run_detect(args)
        return

    if args.command == "export":
        run_export(args)
        return

    if args.command == "export_articles":
        run_export_articles(args)
        return


if __name__ == "__main__":
    main()
