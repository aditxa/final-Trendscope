import csv
from collections import Counter
from pathlib import Path


def main() -> None:
    base = Path("data/daily_trends")
    files = sorted(base.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSVs found in {base}")

    rows_per_day: dict[str, int] = {}
    phrase_day_counts: Counter[str] = Counter()
    canonical_day_counts: Counter[str] = Counter()

    for file_path in files:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)

        day = file_path.stem
        rows_per_day[day] = len(rows)

        seen_phrases: set[str] = set()
        seen_canonicals: set[str] = set()
        for row in rows:
            phrase = (row.get("phrase") or "").strip()
            canonical = (row.get("canonical_phrase") or "").strip()
            if phrase:
                seen_phrases.add(phrase)
            if canonical:
                seen_canonicals.add(canonical)

        for phrase in seen_phrases:
            phrase_day_counts[phrase] += 1
        for canonical in seen_canonicals:
            canonical_day_counts[canonical] += 1

    counts = list(rows_per_day.values())
    nonempty_days = [day for day, n in rows_per_day.items() if n > 0]

    print(f"days_total={len(files)}")
    print(f"days_nonempty={len(nonempty_days)}")
    print(f"rows_min={min(counts)} rows_max={max(counts)} rows_avg={sum(counts)/len(counts):.2f}")

    hist: Counter[int] = Counter(counts)
    print("rows_hist=" + ", ".join(f"{k}:{hist[k]}" for k in sorted(hist)))

    print("\nTop phrases by #days appearing:")
    for phrase, n_days in phrase_day_counts.most_common(15):
        print(f"{n_days:>3}  {phrase}")

    print("\nTop canonical phrases by #days appearing:")
    for canonical, n_days in canonical_day_counts.most_common(15):
        print(f"{n_days:>3}  {canonical}")


if __name__ == "__main__":
    main()
