import argparse
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from gdelt.src.gdelt_client import to_gdelt_timestamp  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge articles from the last N days (inclusive) from the SQLite DB")
    parser.add_argument("--db-path", default="data/gdelt_food.db")
    parser.add_argument("--days", type=int, default=28)
    args = parser.parse_args()

    days = int(args.days)
    if days <= 0:
        raise SystemExit("--days must be >= 1")

    now = datetime.now(tz=timezone.utc)
    end_day = now.date()  # include today
    start_day = end_day - timedelta(days=days - 1)

    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_exclusive = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    start_iso = start_dt.isoformat()
    end_iso = end_exclusive.isoformat()
    start_gdelt = to_gdelt_timestamp(start_dt)
    end_gdelt = to_gdelt_timestamp(end_exclusive)

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM articles
        WHERE (seendate >= ? AND seendate < ?)
           OR (seendate >= ? AND seendate < ?)
        """,
        (start_iso, end_iso, start_gdelt, end_gdelt),
    )
    (to_delete,) = cur.fetchone()

    cur.execute(
        """
        DELETE FROM articles
        WHERE (seendate >= ? AND seendate < ?)
           OR (seendate >= ? AND seendate < ?)
        """,
        (start_iso, end_iso, start_gdelt, end_gdelt),
    )

    conn.commit()
    conn.close()

    print(
        f"Purged {to_delete} articles for {start_day.isoformat()} to {end_day.isoformat()} (inclusive) from {args.db_path}"
    )


if __name__ == "__main__":
    main()
