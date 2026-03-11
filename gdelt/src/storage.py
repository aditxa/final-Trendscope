import hashlib
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from .trend_features import canonicalize_url


@dataclass
class StorageConfig:
    sqlite_path: str = "data/gdelt_food.db"
    use_postgres: bool = False
    postgres_dsn: Optional[str] = None


def _ensure_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _connect_sqlite(path: str) -> sqlite3.Connection:
    _ensure_dir(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _connect_postgres(dsn: str):
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2-binary is required for Postgres support") from exc
    return psycopg2.connect(dsn)


class Storage:
    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        if config.use_postgres:
            if not config.postgres_dsn:
                raise ValueError("postgres_dsn is required when use_postgres is True")
            self.backend = "postgres"
            self.conn = _connect_postgres(config.postgres_dsn)
        else:
            self.backend = "sqlite"
            self.conn = _connect_sqlite(config.sqlite_path)

    def init_db(self) -> None:
        if self.backend == "sqlite":
            self._init_sqlite()
        else:
            self._init_postgres()

        self._ensure_article_columns()
        self._ensure_indexes()

    def _init_sqlite(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                url_canonical TEXT,
                url_hash TEXT NOT NULL,
                content_hash TEXT,
                title TEXT,
                snippet TEXT,
                seendate TEXT,
                domain TEXT,
                language TEXT,
                sourcecountry TEXT,
                socialimage TEXT,
                query_used TEXT,
                inserted_at TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY,
                last_enddatetime TEXT
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS phrase_counts (
                phrase TEXT NOT NULL,
                date TEXT NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (phrase, date)
            )
            """
        )
        self.conn.commit()

    def _init_postgres(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                url_canonical TEXT,
                url_hash TEXT NOT NULL,
                content_hash TEXT,
                title TEXT,
                snippet TEXT,
                seendate TEXT,
                domain TEXT,
                language TEXT,
                sourcecountry TEXT,
                socialimage TEXT,
                query_used TEXT,
                inserted_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY,
                last_enddatetime TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS phrase_counts (
                phrase TEXT NOT NULL,
                date TEXT NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (phrase, date)
            )
            """
        )
        self.conn.commit()

    def _hash_url(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def _hash_content(self, title: Optional[str], snippet: Optional[str]) -> str:
        normalized_title = self._normalize_text(title)
        normalized_snippet = self._normalize_text(snippet)
        payload = f"{normalized_title}::{normalized_snippet}".encode("utf-8")
        return hashlib.sha1(payload).hexdigest()

    @staticmethod
    def _normalize_text(text: Optional[str]) -> str:
        if not text:
            return ""
        return " ".join(text.lower().split())

    def _ensure_article_columns(self) -> None:
        if self.backend != "sqlite":
            return
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(articles)")
        existing = {row[1] for row in cur.fetchall()}
        missing = {
            "url_canonical": "ALTER TABLE articles ADD COLUMN url_canonical TEXT",
            "content_hash": "ALTER TABLE articles ADD COLUMN content_hash TEXT",
            "snippet": "ALTER TABLE articles ADD COLUMN snippet TEXT",
        }
        for column, ddl in missing.items():
            if column not in existing:
                self.conn.execute(ddl)
        self.conn.commit()

    def _ensure_indexes(self) -> None:
        try:
            if self.backend == "sqlite":
                self.conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url_canonical ON articles(url_canonical)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_articles_seendate ON articles(seendate)"
                )
            else:
                cur = self.conn.cursor()
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url_canonical ON articles(url_canonical)"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_seendate ON articles(seendate)")
            self.conn.commit()
        except Exception as exc:
            logging.warning("Could not create indexes: %s", exc)

    def insert_articles(self, articles: Iterable[Dict], query_used: str) -> int:
        rows = []
        inserted_at = datetime.now(tz=timezone.utc).isoformat()

        for article in articles:
            url = article.get("url")
            if not url:
                continue
            url_canonical = canonicalize_url(url)
            content_hash = self._hash_content(article.get("title"), article.get("snippet"))
            rows.append(
                (
                    url,
                    url_canonical,
                    self._hash_url(url),
                    content_hash,
                    article.get("title"),
                    article.get("snippet"),
                    article.get("seendate"),
                    article.get("domain"),
                    article.get("language"),
                    article.get("sourcecountry"),
                    article.get("socialimage"),
                    query_used,
                    inserted_at,
                )
            )

        if not rows:
            return 0

        if self.backend == "sqlite":
            cur = self.conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO articles (
                    url, url_canonical, url_hash, content_hash, title, snippet, seendate, domain, language,
                    sourcecountry, socialimage, query_used, inserted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.conn.commit()
            return cur.rowcount

        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO articles (
                url, url_canonical, url_hash, content_hash, title, snippet, seendate, domain, language,
                sourcecountry, socialimage, query_used, inserted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def get_checkpoint(self) -> Optional[str]:
        cur = self.conn.cursor()
        if self.backend == "sqlite":
            cur.execute("SELECT last_enddatetime FROM checkpoints WHERE id = 1")
        else:
            cur.execute("SELECT last_enddatetime FROM checkpoints WHERE id = 1")
        row = cur.fetchone()
        return row[0] if row else None

    def update_checkpoint(self, enddatetime: str) -> None:
        cur = self.conn.cursor()
        if self.backend == "sqlite":
            cur.execute(
                """
                INSERT INTO checkpoints (id, last_enddatetime)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET last_enddatetime = excluded.last_enddatetime
                """,
                (enddatetime,),
            )
        else:
            cur.execute(
                """
                INSERT INTO checkpoints (id, last_enddatetime)
                VALUES (1, %s)
                ON CONFLICT (id) DO UPDATE SET last_enddatetime = EXCLUDED.last_enddatetime
                """,
                (enddatetime,),
            )
        self.conn.commit()

    def upsert_phrase_counts(self, counts: List[Tuple[str, str, int]]) -> None:
        if not counts:
            return
        cur = self.conn.cursor()
        if self.backend == "sqlite":
            cur.executemany(
                """
                INSERT INTO phrase_counts (phrase, date, count)
                VALUES (?, ?, ?)
                ON CONFLICT(phrase, date) DO UPDATE SET count = excluded.count
                """,
                counts,
            )
        else:
            cur.executemany(
                """
                INSERT INTO phrase_counts (phrase, date, count)
                VALUES (%s, %s, %s)
                ON CONFLICT (phrase, date) DO UPDATE SET count = EXCLUDED.count
                """,
                counts,
            )
        self.conn.commit()

    def load_titles(self) -> List[Tuple[str, str]]:
        cur = self.conn.cursor()
        cur.execute("SELECT title, seendate FROM articles WHERE title IS NOT NULL")
        return cur.fetchall()

    def fetch_phrase_totals(self) -> List[Tuple[str, int]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT phrase, SUM(count) as total_count
            FROM phrase_counts
            GROUP BY phrase
            ORDER BY total_count DESC
            """
        )
        return cur.fetchall()

    def close(self) -> None:
        self.conn.close()


def get_sqlite_connection(config: StorageConfig) -> sqlite3.Connection:
    logging.warning("get_sqlite_connection is deprecated; use Storage instead")
    return _connect_sqlite(config.sqlite_path)
