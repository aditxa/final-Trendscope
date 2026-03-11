import sqlite3
import datetime as dt


def main() -> None:
    conn = sqlite3.connect("data/gdelt_food.db")
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print("tables=", [r[0] for r in cur.fetchall()])

    cur.execute("PRAGMA table_info(phrase_counts)")
    cols = [r[1] for r in cur.fetchall()]
    print("phrase_counts_cols=", cols)

    bucket_col = "bucket_start" if "bucket_start" in cols else ("date" if "date" in cols else None)
    if bucket_col is None:
        raise SystemExit("phrase_counts has no bucket/date column")

    print("bucket_col=", bucket_col)
    cur.execute(f"SELECT MAX({bucket_col}) FROM phrase_counts")
    max_bucket = cur.fetchone()[0]
    print("max_bucket=", max_bucket)

    max_day = dt.date.fromisoformat(max_bucket)
    start_day = (max_day - dt.timedelta(days=6)).isoformat()

    cur.execute(
        f"""
        SELECT phrase, SUM(count) AS c
        FROM phrase_counts
        WHERE {bucket_col} >= ?
        GROUP BY phrase
        ORDER BY c DESC
        LIMIT 30
        """,
        (start_day,),
    )

    print("top_last7d=")
    for phrase, c in cur.fetchall():
        print(int(c), phrase)

    conn.close()


if __name__ == "__main__":
    main()
