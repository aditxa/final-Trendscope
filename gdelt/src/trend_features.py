import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

PUNCT_RE = re.compile(r"[^a-zA-Z0-9\s]")
SPACE_RE = re.compile(r"\s+")
TRACKING_PARAMS = {
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "source",
    "spm",
    "igshid",
}
TRACKING_PREFIXES = ("utm_",)


def normalize_phrase(phrase: str) -> str:
    text = PUNCT_RE.sub(" ", phrase.lower())
    text = SPACE_RE.sub(" ", text).strip()
    if text.endswith(" recipes"):
        text = text[: -len(" recipes")].strip()
    if text.endswith(" recipe"):
        text = text[: -len(" recipe")].strip()
    return text


def _singularize(word: str) -> str:
    if word.endswith("ies") and len(word) > 4:
        if word[:-1].endswith("ie"):
            return word[:-1]
        return word[:-3] + "y"
    if word.endswith("ses") and len(word) > 4:
        return word[:-2]
    if (
        word.endswith("s")
        and not word.endswith("ss")
        and len(word) > 3
        and not word.endswith(("ais", "ois", "us", "is", "ss"))
    ):
        return word[:-1]
    return word


def canonicalize_phrase(phrase: str) -> str:
    normalized = normalize_phrase(phrase)
    parts = [
        _singularize(part)
        for part in normalized.split()
        if part
    ]
    return " ".join(parts)


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url.strip())
    scheme = (parts.scheme or "http").lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""
    if path.endswith("/") and path != "/":
        path = path[:-1]

    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAMS:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PREFIXES):
            continue
        query_items.append((key_lower, value))
    query_items.sort()
    query = urlencode(query_items, doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def bucket_start_from_seendate(seendate: str, bucket: str) -> str:
    if not seendate:
        return ""
    dt = _parse_seendate(seendate)
    if not dt:
        return ""

    if bucket == "hourly":
        dt = dt.replace(minute=0, second=0, microsecond=0)
        return dt.isoformat()
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.date().isoformat()


def _parse_seendate(seendate: str) -> datetime | None:
    try:
        return datetime.fromisoformat(seendate.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.strptime(seendate, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_bucket(bucket_start: str, bucket: str) -> datetime:
    if bucket == "hourly":
        return datetime.fromisoformat(bucket_start).astimezone(timezone.utc)
    return datetime.fromisoformat(bucket_start).replace(tzinfo=timezone.utc)


def compute_window_sums(
    counts_by_time: Dict[datetime, int],
    window_start: datetime,
    window_end: datetime,
    window_delta: timedelta,
) -> List[int]:
    sums = []
    cursor = window_start
    while cursor < window_end:
        next_cursor = cursor + window_delta
        total = 0
        for dt, count in counts_by_time.items():
            if cursor <= dt < next_cursor:
                total += count
        sums.append(total)
        cursor = next_cursor
    return sums


def compute_trend_features(
    phrase_counts: List[Tuple[str, datetime, int]],
    current_window_start: datetime,
    current_window_end: datetime,
    baseline_start: datetime,
    baseline_end: datetime,
    window_delta: timedelta,
) -> Dict[str, Dict]:
    series: Dict[str, Dict[datetime, int]] = defaultdict(lambda: defaultdict(int))
    first_seen: Dict[str, datetime] = {}

    for phrase, bucket_start, count in phrase_counts:
        series[phrase][bucket_start] += count
        if phrase not in first_seen or bucket_start < first_seen[phrase]:
            first_seen[phrase] = bucket_start

    features: Dict[str, Dict] = {}

    for phrase, counts_by_time in series.items():
        current_count = sum(
            count
            for dt, count in counts_by_time.items()
            if current_window_start <= dt < current_window_end
        )
        previous_window_start = current_window_start - window_delta
        previous_window_count = sum(
            count
            for dt, count in counts_by_time.items()
            if previous_window_start <= dt < current_window_start
        )
        previous_previous_start = previous_window_start - window_delta
        previous_previous_count = sum(
            count
            for dt, count in counts_by_time.items()
            if previous_previous_start <= dt < previous_window_start
        )

        baseline_window_sums = compute_window_sums(
            counts_by_time,
            baseline_start,
            baseline_end,
            window_delta,
        )
        baseline_mean = sum(baseline_window_sums) / max(1, len(baseline_window_sums))
        variance = (
            sum((value - baseline_mean) ** 2 for value in baseline_window_sums)
            / max(1, len(baseline_window_sums))
        )
        baseline_std = math.sqrt(variance)
        sorted_samples = sorted(baseline_window_sums)
        median = sorted_samples[len(sorted_samples) // 2] if sorted_samples else 0.0
        mad = (
            sorted(abs(value - median) for value in sorted_samples)[len(sorted_samples) // 2]
            if sorted_samples
            else 0.0
        )

        z_score = (current_count - baseline_mean) / math.sqrt(baseline_mean + 1.0)
        growth_ratio = (current_count + 1) / (baseline_mean + 1)
        velocity = current_count - previous_window_count
        previous_velocity = previous_window_count - previous_previous_count
        acceleration = velocity - previous_velocity
        novelty_days = (current_window_end - first_seen[phrase]).days

        features[phrase] = {
            "current_count": current_count,
            "baseline_mean": baseline_mean,
            "baseline_std": baseline_std,
            "baseline_median": median,
            "baseline_mad": mad,
            "baseline_samples": baseline_window_sums,
            "z_score": z_score,
            "growth_ratio": growth_ratio,
            "velocity": velocity,
            "acceleration": acceleration,
            "novelty_days": novelty_days,
        }

    return features
