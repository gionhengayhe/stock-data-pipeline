from datetime import datetime, time

from scripts.common.config import DATA_ROOT, required_env
from scripts.common.files import dated_file, write_json_atomic
from scripts.common.http import get_json


API_LIMIT = 1000
PUBLISHED_TIME_FORMATS = ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M")


def _news_key(row: dict) -> tuple:
    return row.get("url"), row.get("time_published"), row.get("title")


def _published_at(row: dict) -> datetime:
    value = row.get("time_published")
    if not isinstance(value, str):
        raise RuntimeError(
            "Alpha Vantage returned a full news page without a valid "
            "time_published cursor"
        )

    for date_format in PUBLISHED_TIME_FORMATS:
        try:
            return datetime.strptime(value, date_format)
        except ValueError:
            continue

    raise RuntimeError(
        f"Alpha Vantage returned an unsupported time_published value: {value!r}"
    )


def _fetch_news_range(start: datetime, end: datetime, api_key: str) -> list[dict]:
    """Fetch a news range newest-first using the oldest row as the next cursor."""
    cursor_end = end.replace(second=0, microsecond=0)
    rows = []
    seen = set()
    request_count = 0

    while cursor_end >= start:
        payload = get_json(
            "https://www.alphavantage.co/query",
            params={
                "function": "NEWS_SENTIMENT",
                "time_from": start.strftime("%Y%m%dT%H%M"),
                "time_to": cursor_end.strftime("%Y%m%dT%H%M"),
                "sort": "LATEST",
                "limit": API_LIMIT,
                "apikey": api_key,
            },
            required_key="feed",
        )
        request_count += 1
        feed = payload["feed"]
        if not isinstance(feed, list):
            raise RuntimeError("Alpha Vantage news response 'feed' must be a list")

        for row in feed:
            key = _news_key(row)
            if key not in seen:
                seen.add(key)
                rows.append(row)

        if len(feed) < API_LIMIT:
            break

        oldest_published_at = min(_published_at(row) for row in feed)
        next_cursor_end = oldest_published_at.replace(second=0, microsecond=0)
        if next_cursor_end >= cursor_end:
            raise RuntimeError(
                "Alpha Vantage returned a full news page without an older "
                "timestamp; cursor pagination cannot continue without risking "
                "missing or repeated data"
            )
        cursor_end = next_cursor_end

    print(
        f"Fetched {len(rows)} unique news items using "
        f"{request_count} Alpha Vantage request(s)"
    )
    return rows


def crawl_news(**kwargs):
    execution_date = kwargs["execution_date"]
    api_key = required_env("ALPHA_VANTAGE_API_KEY")

    day = execution_date.date()
    start = datetime.combine(day, time.min)
    end = datetime.combine(day, time(23, 59))
    rows = _fetch_news_range(start, end, api_key)

    # Keep this final guard because cursor boundaries are intentionally inclusive.
    deduplicated = {}
    for row in rows:
        deduplicated[_news_key(row)] = row
    news = sorted(
        deduplicated.values(),
        key=lambda row: (
            row.get("time_published") or "",
            row.get("url") or "",
            row.get("title") or "",
        ),
        reverse=True,
    )

    path = dated_file(
        DATA_ROOT / "raw" / "news", "crawl_news", execution_date, ".json"
    )

    write_json_atomic(news, path)

    # Print success message with total news items and file path
    print(f"The process of crawling {len(news)} unique news items was successful")
    print(f"Saving at {path}")
