import argparse
import csv
import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Tuple


def _iter_daily_csvs(folder: Path, month: str) -> List[Path]:
    return sorted(folder.glob(f"{month}-*.csv"))


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


def _safe_load_articles(value: str | None) -> List[Dict[str, str]]:
    text = (value or "").strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(loaded, list):
        out: List[Dict[str, str]] = []
        for item in loaded:
            if isinstance(item, dict):
                out.append({k: str(v) for k, v in item.items()})
        return out
    return []


def collect_from_daily(folder: Path, month: str, phrase: str) -> List[Dict[str, str]]:
    phrase_norm = phrase.strip().lower()

    by_url: "OrderedDict[str, Dict[str, str]]" = OrderedDict()

    for p in _iter_daily_csvs(folder, month):
        day = p.name[:10]
        with p.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_phrase = (row.get("phrase") or "").strip().lower()
                if row_phrase != phrase_norm:
                    continue

                titles = _safe_load_json_list(row.get("sample_titles"))
                urls = _safe_load_json_list(row.get("sample_urls"))
                for title, url in zip(titles, urls):
                    url = str(url).strip()
                    if not url or url in by_url:
                        continue
                    by_url[url] = {"date": day, "title": str(title).strip(), "url": url}

    return list(by_url.values())


def collect_from_monthly_phrases(csv_path: Path, month: str, phrase: str) -> List[Dict[str, str]]:
    phrase_norm = phrase.strip().lower()
    month_norm = month.strip()

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if (row.get("month") or "").strip() != month_norm:
                continue
            if (row.get("phrase") or "").strip().lower() != phrase_norm:
                continue
            return _safe_load_articles(row.get("articles_json"))

    return []


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Show all sample articles for a phrase")
    p.add_argument("--month", required=True, help="Month in YYYY-MM (e.g. 2026-01)")
    p.add_argument("--phrase", required=True, help="Phrase to look up (case-insensitive)")
    p.add_argument(
        "--folder",
        default=None,
        help="Folder containing daily CSVs (if set, scans daily CSVs for full list)",
    )
    p.add_argument(
        "--monthly-phrases",
        default="data/monthly_phrases_foodonly_window3.csv",
        help="Monthly phrases CSV to read (used if --folder not set)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    month = args.month
    phrase = args.phrase

    if args.folder:
        articles = collect_from_daily(Path(args.folder), month, phrase)
        source = f"daily CSVs in {args.folder}"
    else:
        articles = collect_from_monthly_phrases(Path(args.monthly_phrases), month, phrase)
        source = f"{args.monthly_phrases}"

    print(f"Phrase: {phrase}")
    print(f"Month:  {month}")
    print(f"From:   {source}")
    print(f"Found:  {len(articles)} article(s)\n")

    for i, a in enumerate(articles, start=1):
        date = a.get("date", "")
        title = a.get("title", "")
        url = a.get("url", "")
        print(f"{i:>3}. {date} | {title}\n     {url}\n")


if __name__ == "__main__":
    main()
