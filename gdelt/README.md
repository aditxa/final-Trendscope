# TrendScope GDELT Food Trends (MVP)

A minimal GDELT 2.0 DOC API ingestion pipeline to discover trending/viral food items and recipes.

## Features
- GDELT DOC 2.0 ArtList ingestion with adaptive window splitting
- SQLite storage + dedupe by canonical URL and content hash
- Incremental checkpoints
- Phrase extraction from titles (2-gram, 3-gram) + noun-phrases (spaCy if available)
- Time-bucket phrase counts (hourly/daily)
- Trend detection with z/growth/velocity/novelty and gating
- CSV/JSON export with evidence articles

## Setup
### Recommended: Python 3.12 + venv

spaCy currently does not work on Python 3.14 in this repo's dependency stack. Use Python 3.12 for best results.

1) Create a virtual environment
```
py -3.12 -m venv gdelt/.venv
```

2) Install dependencies
```
gdelt\.venv\Scripts\python.exe -m pip install -U pip
gdelt\.venv\Scripts\python.exe -m pip install -r gdelt/requirements.txt
```

3) Download the spaCy language model (optional but recommended)
```
gdelt\.venv\Scripts\python.exe -m spacy download en_core_web_sm
```

## Usage
### Run tests
```
gdelt\.venv\Scripts\python.exe -m pytest gdelt/tests/
```

### Recommended production run (build a real baseline)
1) Backfill enough history (example: last 28 days)
```
python -m gdelt.src.main backfill --start 2025-12-31 --end 2026-01-27
```

2) Build daily counts over the full article range
```
python -m gdelt.src.main build_counts --bucket daily
```

3) Detect using a baseline that matches your backfill window (example: 28 days)
```
python -m gdelt.src.main detect --bucket daily --current-window-hours 24 --baseline-days 28 --top-k 50
```

4) Export
```
python -m gdelt.src.main export --format csv --output data/trends_export.csv --top-k 50
```
### Run once (last N minutes)
```
python -m gdelt.src.main run_once --minutes 60
```

### Backfill a date range (inclusive)
```
python -m gdelt.src.main backfill --start-date 2025-01-01 --end-date 2025-01-07
```

### Extract phrase candidates
```
python -m gdelt.src.main extract --csv-path data/phrase_candidates.csv
```

### Build phrase counts for trend detection
```
python -m gdelt.src.main build_counts --bucket daily
```

### Detect trending phrases
```
python -m gdelt.src.main detect --bucket daily --current-window-hours 24 --baseline-days 14 --top-k 50 \
	--z-threshold 2.0 --burst-percentile 0.9
```

### Export trends
```
python -m gdelt.src.main export --format csv --output data/trends_export.csv
```

### Optional: Postgres
Install the optional dependency and pass the flags:
```
pip install psycopg2-binary
python -m gdelt.src.main run_once --use-postgres --postgres-dsn "postgresql://user:pass@localhost:5432/gdelt"
```

## Configuration
Common flags:
- `--db-path` (default: data/gdelt_food.db)
- `--language` (default: English)
- `--source-country` (optional)
- `--query` (default: discovery query)
- `--maxrecords` (default: 250)
- `--min-window-seconds` (default: 900)
- `--rate-limit-seconds` (default: 5.0)
- `--use-postgres` (optional)
- `--postgres-dsn` (optional)

Trend detection flags:
- `--bucket` (daily|hourly)
- `--current-window-hours` (default: 24)
- `--baseline-days` (default: 14)
- `--top-k` (default: 50)
- `--min-total` (default: 5)
- `--min-score` (default: 6.0)
- `--weights` (default: z=1.0,g=0.8,v=0.6,n=0.4)
- `--evidence-n` (default: 5)
- `--z-threshold` (default: 2.0)
- `--burst-percentile` (default: 0.9)

## Notes
- GDELT DOC API returns at most 250 records per request. The client splits time windows when a window hits the cap.
- Titles are used for MVP phrase extraction; consider adding body parsing later.

## Trend score interpretation
The trend score is a weighted sum of:
- z-score of current vs baseline window
- log growth delta (log1p now - log1p baseline)
- log velocity (current vs previous window)
- novelty bonus for recently first-seen phrases

Gates:
- minimum support (`--min-total`)
- burst gate: either z-score >= `--z-threshold` or current count exceeds the baseline percentile

The score is multiplied by a volume confidence factor to down-weight tiny counts.
