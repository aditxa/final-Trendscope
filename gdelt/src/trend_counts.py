import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .extract_phrases import (
    DISH_SUFFIXES,
    FOOD_LEXICON,
    GENERIC_PHRASES,
    STOPWORDS,
    TOKEN_RE,
    extract_phrases_from_title,
)
from .gdelt_client import to_gdelt_timestamp
from .trend_features import normalize_phrase, bucket_start_from_seendate
from .trend_storage import ensure_phrase_counts_schema, upsert_phrase_counts


def _tokenize(title: str) -> List[str]:
    tokens = [token.lower() for token in TOKEN_RE.findall(title or "")]
    return [token for token in tokens if token not in STOPWORDS and len(token) > 2]


def _ngrams(tokens: Sequence[str], n: int) -> Iterable[str]:
    if len(tokens) < n:
        return []
    return [" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


def _is_food_phrase(phrase: str) -> bool:
    tokens = phrase.split()
    if not tokens:
        return False
    if all(token in STOPWORDS for token in tokens):
        return False
    if phrase in FOOD_LEXICON:
        return True
    if any(token in FOOD_LEXICON for token in tokens):
        return True
    if len(tokens) >= 2:
        for n in (2, 3):
            if len(tokens) >= n:
                for i in range(len(tokens) - n + 1):
                    if " ".join(tokens[i : i + n]) in FOOD_LEXICON:
                        return True
    if tokens[-1] in DISH_SUFFIXES:
        return True
    return False


def build_phrase_counts(
    conn,
    bucket: str = "daily",
    food_only: bool = False,
    include_unigrams: bool = False,
    method: str = "ngrams",
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> int:
    if method not in {"ngrams", "spacy"}:
        raise ValueError("method must be 'ngrams' or 'spacy'")

    cur = conn.cursor()

    # This operation is a rebuild: remove existing rows in the rebuilt range so
    # results don't accumulate across runs.
    bucket_column = ensure_phrase_counts_schema(conn)
    if start_dt is not None and end_dt is not None:
        if bucket == "hourly":
            start_key = start_dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
            end_key = end_dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc).isoformat()
        else:
            start_key = start_dt.date().isoformat()
            end_key = end_dt.date().isoformat()
        cur.execute(
            f"DELETE FROM phrase_counts WHERE {bucket_column} >= ? AND {bucket_column} < ?",
            (start_key, end_key),
        )
    else:
        cur.execute("DELETE FROM phrase_counts")
    conn.commit()

    sql = "SELECT title, seendate, domain FROM articles WHERE title IS NOT NULL"
    params: List[str] = []
    if start_dt is not None and end_dt is not None:
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()
        start_gdelt = to_gdelt_timestamp(start_dt)
        end_gdelt = to_gdelt_timestamp(end_dt)
        sql += " AND ((seendate >= ? AND seendate < ?) OR (seendate >= ? AND seendate < ?))"
        params.extend([start_iso, end_iso, start_gdelt, end_gdelt])

    cur.execute(sql, params)
    rows = cur.fetchall()

    counts: Counter = Counter()
    seen_domain_titles_by_bucket: Dict[str, set[tuple[str, str]]] = {}
    for title, seendate, domain in rows:
        if method == "spacy":
            grams = extract_phrases_from_title(title)
        else:
            tokens = _tokenize(title)
            grams = []
            if food_only or include_unigrams:
                grams.extend(tokens)
            grams.extend(_ngrams(tokens, 2))
            grams.extend(_ngrams(tokens, 3))

        bucket_start = bucket_start_from_seendate(seendate, bucket)
        if not bucket_start:
            continue

        # Deduplicate within a site: only count each (domain,title) once per bucket.
        # This preserves cross-site pickup (a useful proxy for reach) while
        # preventing duplicates from the same domain inflating counts.
        title_key = (title or "").strip().lower()
        domain_key = (domain or "").strip().lower()
        if title_key and domain_key:
            seen = seen_domain_titles_by_bucket.setdefault(bucket_start, set())
            key = (domain_key, title_key)
            if key in seen:
                continue
            seen.add(key)

        # Avoid double-counting the same phrase multiple times within a title.
        grams = list(dict.fromkeys([g for g in grams if g]))

        for gram in grams:
            normalized = normalize_phrase(gram)
            if not normalized:
                continue
            if normalized in GENERIC_PHRASES:
                continue
            # spaCy extraction already applies food/quality filtering; keep this
            # gate for the n-gram method (and as a safety valve if needed).
            if food_only and not _is_food_phrase(normalized):
                continue
            counts[(normalized, bucket_start)] += 1

    upsert_rows = [(phrase, bucket_start, count) for (phrase, bucket_start), count in counts.items()]
    upsert_phrase_counts(conn, upsert_rows, bucket_column)

    logging.info("Built %s phrase bucket counts", len(upsert_rows))
    return len(upsert_rows)
