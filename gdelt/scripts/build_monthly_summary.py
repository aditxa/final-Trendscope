import argparse
import csv
import glob
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass
class PhraseAgg:
    best_score: float = 0.0
    best_date: str = ""
    total_current_count: int = 0
    appearances: int = 0
    articles_by_url: Dict[str, Dict[str, str]] = field(default_factory=dict)


def _iter_daily_csvs(folder: str, month: str) -> List[str]:
    pattern = os.path.join(folder, f"{month}-*.csv")
    return sorted(glob.glob(pattern))


def _safe_float(value: str | None) -> float:
    try:
        return float(value or 0.0)
    except ValueError:
        return 0.0


def _safe_int(value: str | None) -> int:
    try:
        return int(float(value or 0))
    except ValueError:
        return 0


def _safe_load_json_list(value: str | None) -> List[str]:
    text = (value or "").strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(loaded, list):
        return [str(x) for x in loaded]
    return []


def summarize_month(
    month: str,
    folder: str,
    *,
    max_articles_per_phrase: int,
) -> tuple[Dict[str, object], List[Dict[str, object]]]:
    paths = _iter_daily_csvs(folder, month)
    total_days = len(paths)
    nonempty_days = 0

    phrases: Dict[str, PhraseAgg] = defaultdict(PhraseAgg)

    for p in paths:
        date = os.path.basename(p)[:10]
        with open(p, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            continue
        nonempty_days += 1

        seen_phrases_today = set()
        for r in rows:
            phrase = (r.get("phrase") or "").strip()
            if not phrase:
                continue

            score = _safe_float(r.get("trend_score"))
            current_count = _safe_int(r.get("current_count"))

            agg = phrases[phrase]
            agg.total_current_count += current_count
            if phrase not in seen_phrases_today:
                agg.appearances += 1
                seen_phrases_today.add(phrase)

            if score > agg.best_score:
                agg.best_score = score
                agg.best_date = date

            if max_articles_per_phrase != 0 and (
                max_articles_per_phrase < 0 or len(agg.articles_by_url) < max_articles_per_phrase
            ):
                titles = _safe_load_json_list(r.get("sample_titles"))
                urls = _safe_load_json_list(r.get("sample_urls"))
                for title, url in zip(titles, urls):
                    url = str(url).strip()
                    if not url:
                        continue
                    if url in agg.articles_by_url:
                        continue
                    agg.articles_by_url[url] = {
                        "date": date,
                        "title": str(title).strip(),
                        "url": url,
                    }
                    if max_articles_per_phrase > 0 and len(agg.articles_by_url) >= max_articles_per_phrase:
                        break

    phrase_items: List[Tuple[str, PhraseAgg]] = sorted(
        phrases.items(),
        key=lambda item: (item[1].best_score, item[1].appearances, item[1].total_current_count, item[0]),
        reverse=True,
    )

    phrases_json = json.dumps(
        [
            {
                "phrase": phrase,
                "best_score": round(agg.best_score, 6),
                "best_date": agg.best_date,
                "appearances": agg.appearances,
                "total_current_count": agg.total_current_count,
                "articles_count": len(agg.articles_by_url),
                "articles": sorted(
                    agg.articles_by_url.values(),
                    key=lambda a: (a.get("date", ""), a.get("url", "")),
                ),
            }
            for phrase, agg in phrase_items
        ],
        ensure_ascii=False,
    )

    top_phrase = phrase_items[0][0] if phrase_items else ""
    top_agg = phrase_items[0][1] if phrase_items else PhraseAgg()

    summary = {
        "month": month,
        "out_dir": folder.replace("\\", "/"),
        "total_days": total_days,
        "nonempty_days": nonempty_days,
        "phrases_count": len(phrase_items),
        "top_phrase": top_phrase,
        "top_score": round(top_agg.best_score, 6),
        "top_date": top_agg.best_date,
        "top_appearances": top_agg.appearances,
        "top_total_current_count": top_agg.total_current_count,
        "phrases_json": phrases_json,
    }

    phrase_rows = [
        {
            "month": month,
            "out_dir": folder.replace("\\", "/"),
            "phrase": phrase,
            "best_score": round(agg.best_score, 6),
            "best_date": agg.best_date,
            "appearances": agg.appearances,
            "total_current_count": agg.total_current_count,
            "articles_count": len(agg.articles_by_url),
            "articles_json": json.dumps(
                sorted(
                    agg.articles_by_url.values(),
                    key=lambda a: (a.get("date", ""), a.get("url", "")),
                ),
                ensure_ascii=False,
            ),
        }
        for phrase, agg in phrase_items
    ]

    return summary, phrase_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build monthly summary CSV from daily trend CSVs")
    parser.add_argument(
        "--month",
        action="append",
        dest="months",
        help="Month to summarize in YYYY-MM format (repeatable)",
    )
    parser.add_argument(
        "--folder",
        action="append",
        dest="folders",
        help="Folder containing daily CSVs for the corresponding --month (repeatable)",
    )
    parser.add_argument(
        "--out",
        default="data/monthly_summary_foodonly_window3.csv",
        help="Output CSV path (default: data/monthly_summary_foodonly_window3.csv)",
    )
    parser.add_argument(
        "--out-phrases",
        default="data/monthly_phrases_foodonly_window3.csv",
        help="Output per-phrase CSV path (default: data/monthly_phrases_foodonly_window3.csv)",
    )
    parser.add_argument(
        "--out-phrase-articles",
        default="data/monthly_phrase_articles_foodonly_window3.csv",
        help=(
            "Output long-format CSV with one row per (month, phrase, article) "
            "(default: data/monthly_phrase_articles_foodonly_window3.csv)"
        ),
    )
    parser.add_argument(
        "--max-articles-per-phrase",
        type=int,
        default=-1,
        help=(
            "Max sample articles to keep per phrase across the month "
            "(default: -1 unlimited; 0 disables; N>0 limits)"
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    months = args.months or ["2025-11", "2025-12", "2026-01"]
    folders = args.folders or [
        "data/daily_trends_2025-11_window3_foodonly",
        "data/daily_trends_2025-12_window3_foodonly",
        "data/daily_trends_2026-01_window3_foodonly",
    ]

    if len(months) != len(folders):
        raise SystemExit("Provide the same number of --month and --folder arguments")

    month_rows: List[Dict[str, object]] = []
    phrase_rows: List[Dict[str, object]] = []
    for month, folder in zip(months, folders):
        summary, phrases = summarize_month(
            month,
            folder,
            max_articles_per_phrase=int(args.max_articles_per_phrase),
        )
        month_rows.append(summary)
        phrase_rows.extend(phrases)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(month_rows[0].keys()))
        writer.writeheader()
        writer.writerows(month_rows)

    out_phrases_path = Path(args.out_phrases)
    out_phrases_path.parent.mkdir(parents=True, exist_ok=True)
    with out_phrases_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "month",
                "out_dir",
                "phrase",
                "best_score",
                "best_date",
                "appearances",
                "total_current_count",
                "articles_count",
                "articles_json",
            ],
        )
        writer.writeheader()
        writer.writerows(phrase_rows)

    out_phrase_articles_path = Path(args.out_phrase_articles)
    out_phrase_articles_path.parent.mkdir(parents=True, exist_ok=True)
    article_rows: List[Dict[str, object]] = []
    for pr in phrase_rows:
        month = str(pr.get("month") or "")
        out_dir = str(pr.get("out_dir") or "")
        phrase = str(pr.get("phrase") or "")
        phrase_best_score = pr.get("best_score")
        phrase_best_date = str(pr.get("best_date") or "")
        articles_json = str(pr.get("articles_json") or "").strip()
        if not articles_json:
            continue
        try:
            articles = json.loads(articles_json)
        except json.JSONDecodeError:
            continue
        if not isinstance(articles, list):
            continue
        for a in articles:
            if not isinstance(a, dict):
                continue
            article_rows.append(
                {
                    "month": month,
                    "out_dir": out_dir,
                    "phrase": phrase,
                    "phrase_best_score": phrase_best_score,
                    "phrase_best_date": phrase_best_date,
                    "article_date": str(a.get("date") or ""),
                    "article_title": str(a.get("title") or ""),
                    "article_url": str(a.get("url") or ""),
                }
            )

    with out_phrase_articles_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "month",
                "out_dir",
                "phrase",
                "phrase_best_score",
                "phrase_best_date",
                "article_date",
                "article_title",
                "article_url",
            ],
        )
        writer.writeheader()
        writer.writerows(article_rows)

    print(f"Wrote {out_path}")
    print(f"Wrote {out_phrases_path}")
    print(f"Wrote {out_phrase_articles_path}")


if __name__ == "__main__":
    main()
