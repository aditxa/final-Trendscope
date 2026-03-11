import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple
from typing import Dict

from .gdelt_client import to_gdelt_timestamp


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def init_trend_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phrase_counts (
            phrase TEXT NOT NULL,
            bucket_start TEXT NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (phrase, bucket_start)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trend_runs (
            run_id TEXT PRIMARY KEY,
            params_json TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trends (
            run_id TEXT NOT NULL,
            phrase TEXT NOT NULL,
            canonical_phrase TEXT NOT NULL,
            trend_score REAL NOT NULL,
            current_count INTEGER NOT NULL,
            baseline_mean REAL NOT NULL,
            baseline_std REAL NOT NULL,
            z_score REAL NOT NULL,
            growth_ratio REAL NOT NULL,
            velocity REAL NOT NULL,
            novelty_days REAL NOT NULL,
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            baseline_start TEXT NOT NULL,
            baseline_end TEXT NOT NULL,
            sample_titles TEXT NOT NULL,
            sample_urls TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def ensure_phrase_counts_schema(conn: sqlite3.Connection) -> str:
    columns = _table_columns(conn, "phrase_counts")
    if "bucket_start" in columns:
        return "bucket_start"
    if "date" in columns:
        logging.warning("phrase_counts uses legacy 'date' column; treating it as bucket_start")
        return "date"
    raise RuntimeError("phrase_counts table is missing bucket_start/date column")


def upsert_phrase_counts(
    conn: sqlite3.Connection,
    rows: Iterable[Tuple[str, str, int]],
    bucket_column: str,
) -> None:
    if bucket_column not in {"bucket_start", "date"}:
        raise ValueError("Invalid bucket column")

    sql = (
        "INSERT INTO phrase_counts (phrase, {bucket}, count) VALUES (?, ?, ?) "
        "ON CONFLICT(phrase, {bucket}) DO UPDATE SET count = excluded.count"
    ).format(bucket=bucket_column)

    conn.executemany(sql, list(rows))
    conn.commit()


def insert_trend_run(conn: sqlite3.Connection, run_id: str, params: Dict) -> None:
    created_at = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO trend_runs (run_id, params_json, created_at) VALUES (?, ?, ?)",
        (run_id, json.dumps(params, sort_keys=True), created_at),
    )
    conn.commit()


def insert_trends(conn: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO trends (
            run_id, phrase, canonical_phrase, trend_score, current_count,
            baseline_mean, baseline_std, z_score, growth_ratio, velocity,
            novelty_days, window_start, window_end, baseline_start, baseline_end,
            sample_titles, sample_urls, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(rows),
    )
    conn.commit()


def load_phrase_counts(
    conn: sqlite3.Connection,
    bucket_column: str,
) -> List[Tuple[str, str, int]]:
    cur = conn.cursor()
    cur.execute(f"SELECT phrase, {bucket_column}, count FROM phrase_counts")
    return cur.fetchall()


def fetch_sample_articles(
    conn: sqlite3.Connection,
    phrase: str,
    window_start: str,
    window_end: str,
    limit: int = 5,
) -> List[Tuple[str, str, str, str]]:
    gdelt_start = _maybe_to_gdelt(window_start)
    gdelt_end = _maybe_to_gdelt(window_end)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT title, url, seendate, domain FROM articles
        WHERE lower(title) LIKE ?
          AND (
                (seendate >= ? AND seendate < ?)
             OR (seendate >= ? AND seendate < ?)
          )
        ORDER BY seendate DESC
        LIMIT ?
        """,
        (f"%{phrase.lower()}%", window_start, window_end, gdelt_start, gdelt_end, limit),
    )
    return cur.fetchall()


def fetch_articles_in_window(
    conn: sqlite3.Connection,
    window_start: str,
    window_end: str,
) -> List[Tuple[str, str, str, str]]:
    """Fetch all articles within [window_start, window_end).

    Returns (title, url, seendate, domain). This supports both ISO and GDELT
    timestamp formats, mirroring the filtering used elsewhere in this module.
    """

    gdelt_start = _maybe_to_gdelt(window_start)
    gdelt_end = _maybe_to_gdelt(window_end)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT title, url, seendate, domain FROM articles
        WHERE title IS NOT NULL
          AND (
                (seendate >= ? AND seendate < ?)
             OR (seendate >= ? AND seendate < ?)
          )
        ORDER BY seendate DESC
        """,
        (window_start, window_end, gdelt_start, gdelt_end),
    )
    return cur.fetchall()


def count_distinct_domains(
    conn: sqlite3.Connection,
    phrase: str,
    window_start: str,
    window_end: str,
) -> int:
    gdelt_start = _maybe_to_gdelt(window_start)
    gdelt_end = _maybe_to_gdelt(window_end)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(DISTINCT domain)
        FROM articles
        WHERE lower(title) LIKE ?
          AND (
                (seendate >= ? AND seendate < ?)
             OR (seendate >= ? AND seendate < ?)
          )
        """,
        (f"%{phrase.lower()}%", window_start, window_end, gdelt_start, gdelt_end),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def count_distinct_titles(
    conn: sqlite3.Connection,
    phrase: str,
    window_start: str,
    window_end: str,
) -> int:
    """Count distinct (case-insensitive) titles matching phrase in window.

    This is a lightweight proxy for "unique stories" that helps suppress
    syndicated reposts where the same title appears across many domains.
    """

    gdelt_start = _maybe_to_gdelt(window_start)
    gdelt_end = _maybe_to_gdelt(window_end)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(DISTINCT lower(title))
        FROM articles
        WHERE lower(title) LIKE ?
          AND (
                (seendate >= ? AND seendate < ?)
             OR (seendate >= ? AND seendate < ?)
          )
        """,
        (f"%{phrase.lower()}%", window_start, window_end, gdelt_start, gdelt_end),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _maybe_to_gdelt(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return to_gdelt_timestamp(dt)


def latest_run_id(conn: sqlite3.Connection) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT run_id FROM trend_runs ORDER BY created_at DESC LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def load_trends(conn: sqlite3.Connection, run_id: str, top_k: int) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT phrase, canonical_phrase, trend_score, current_count, baseline_mean, baseline_std,
               z_score, growth_ratio, velocity, novelty_days, sample_titles, sample_urls
        FROM trends
        WHERE run_id = ?
        ORDER BY trend_score DESC
        LIMIT ?
        """,
        (run_id, top_k),
    )
    return cur.fetchall()
