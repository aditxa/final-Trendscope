import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from tenacity import Retrying, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_exponential

GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


@dataclass
class GdeltConfig:
    query: str
    language: str = "English"
    source_country: Optional[str] = None
    max_records: int = 250
    timeout_seconds: int = 20
    max_retries: int = 5
    backoff_base_seconds: float = 1.0
    rate_limit_seconds: float = 5.0
    min_window_seconds: int = 900


def to_gdelt_timestamp(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S")


def _normalize_language(language: str) -> str:
    value = language.strip()
    if not value:
        return ""
    lowered = value.lower()
    if lowered in {"eng", "en", "english"}:
        return "eng"
    return value


def _build_query(base_query: str, language: Optional[str], source_country: Optional[str]) -> str:
    query = base_query.strip()
    if language:
        normalized = _normalize_language(language)
        # GDELT DOC API uses `sourcelang:` for language filtering.
        query = f"{query} AND sourcelang:{normalized}"
    if source_country:
        query = f"{query} AND sourcecountry:{source_country}"
    return query


_KEYWORDS_TOO_SHORT_RE = re.compile(r"keywords were too short\s*:?\s*(?P<kw>.*)$", re.IGNORECASE)


def _error_indicates_invalid_language_filter(response_text: str) -> bool:
    text = (response_text or "").strip()
    if not text:
        return False
    match = _KEYWORDS_TOO_SHORT_RE.search(text)
    if not match:
        return False
    listed = match.group("kw").lower()
    # Some GDELT error pages echo the whole query; try to isolate the actual
    # short keyword list when present.
    for token in re.split(r"[\s,]+", listed):
        cleaned = token.strip("\"'()[]{}:;")
        if cleaned in {"language", "lang", "sourcelang"}:
            return True
    return False


def _should_retry_response(response: requests.Response) -> bool:
    return response.status_code in {429, 500, 502, 503, 504}


def _request_with_retries(params: Dict[str, str], config: GdeltConfig) -> Dict:
    headers = {
        "User-Agent": "TrendScopeGDELT/1.0 (+https://example.local)",
        "Accept": "application/json",
    }
    retrying = Retrying(
        retry=(retry_if_exception_type(requests.RequestException) | retry_if_result(_should_retry_response)),
        wait=wait_exponential(multiplier=config.backoff_base_seconds, min=1, max=30),
        stop=stop_after_attempt(config.max_retries),
        reraise=True,
    )

    for attempt in retrying:
        with attempt:
            response = requests.get(
                GDELT_DOC_API_URL,
                params=params,
                headers=headers,
                timeout=config.timeout_seconds,
            )
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    response_text = response.text.strip()
                    if _error_indicates_invalid_language_filter(response_text):
                        logging.warning(
                            "GDELT rejected the query sourcelang clause; retrying without it (body=%s)",
                            response_text[:200].replace("\n", " "),
                        )
                        return {"_invalid_language": True}
                    if "Timespan is too short" in response_text:
                        logging.warning("GDELT rejected window as too short")
                        return {"articles": []}
                    logging.warning(
                        "GDELT returned non-JSON response: %s",
                        response_text[:500],
                    )
                    return {"articles": []}

            logging.warning("GDELT response status=%s body=%s", response.status_code, response.text[:500])
            return response

    raise RuntimeError("GDELT request failed after retries")


def fetch_window(
    start_dt: datetime,
    end_dt: datetime,
    config: GdeltConfig,
) -> Tuple[List[Dict], int]:
    query = _build_query(config.query, config.language, config.source_country)

    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(config.max_records),
        "startdatetime": to_gdelt_timestamp(start_dt),
        "enddatetime": to_gdelt_timestamp(end_dt),
        "sort": "HybridRel",
    }

    payload = _request_with_retries(params, config)
    if isinstance(payload, dict) and payload.get("_invalid_language"):
        logging.warning("Retrying window without language filter")
        params["query"] = _build_query(config.query, None, config.source_country)
        payload = _request_with_retries(params, config)

    articles = payload.get("articles", []) if isinstance(payload, dict) else []
    return articles, len(articles)


def _split_window(start_dt: datetime, end_dt: datetime) -> Tuple[datetime, datetime]:
    midpoint = start_dt + (end_dt - start_dt) / 2
    return start_dt, midpoint


def adaptive_fetch(
    start_dt: datetime,
    end_dt: datetime,
    config: GdeltConfig,
) -> Iterable[Dict]:
    stack: List[Tuple[datetime, datetime]] = [(start_dt, end_dt)]

    while stack:
        window_start, window_end = stack.pop()

        if (window_end - window_start).total_seconds() < config.min_window_seconds:
            logging.warning("Minimum window reached for %s - %s", window_start, window_end)
            articles, _ = fetch_window(window_start, window_end, config)
            for article in articles:
                yield article
            continue

        articles, count = fetch_window(window_start, window_end, config)

        if count >= config.max_records:
            logging.info("Window hit maxrecords, splitting %s - %s", window_start, window_end)
            left_start, left_end = _split_window(window_start, window_end)
            right_start = left_end
            right_end = window_end
            stack.append((right_start, right_end))
            stack.append((left_start, left_end))
        else:
            for article in articles:
                yield article

        time.sleep(config.rate_limit_seconds)


def generate_run_once_window(minutes: int, overlap_minutes: int = 5) -> Tuple[datetime, datetime]:
    end_dt = datetime.now(tz=timezone.utc)
    start_dt = end_dt - timedelta(minutes=minutes)
    start_dt -= timedelta(minutes=overlap_minutes)
    return start_dt, end_dt
