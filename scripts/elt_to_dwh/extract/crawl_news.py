import os
from datetime import datetime, time, timedelta

from scripts.common.files import write_json_atomic
from scripts.common.http import get_json


API_LIMIT = 1000
MAX_SPLIT_DEPTH = 4


def _fetch_news_range(start: datetime, end: datetime, api_key: str, depth: int = 0) -> list[dict]:
    payload = get_json(
        "https://www.alphavantage.co/query",
        params={
            "function": "NEWS_SENTIMENT",
            "time_from": start.strftime("%Y%m%dT%H%M"),
            "time_to": end.strftime("%Y%m%dT%H%M"),
            "limit": API_LIMIT,
            "apikey": api_key,
        },
        required_key="feed",
    )
    feed = payload["feed"]
    if len(feed) < API_LIMIT:
        return feed
    if depth >= MAX_SPLIT_DEPTH or start >= end:
        raise RuntimeError(
            "Alpha Vantage news result reached the API limit after recursive splitting; "
            "the day cannot be loaded without truncation"
        )

    midpoint = start + (end - start) / 2
    right_start = midpoint + timedelta(minutes=1)
    return _fetch_news_range(start, midpoint, api_key, depth + 1) + _fetch_news_range(
        right_start, end, api_key, depth + 1
    )


def crawl_news(**kwargs):
    execution_date = kwargs["execution_date"]
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is required")

    day = execution_date.date()
    start = datetime.combine(day, time.min)
    end = datetime.combine(day, time(23, 59))
    rows = _fetch_news_range(start, end, api_key)

    # A news item may be returned in adjacent API windows. Keep one natural-key copy.
    deduplicated = {}
    for row in rows:
        key = (row.get("url"), row.get("time_published"), row.get("title"))
        deduplicated[key] = row
    news = list(deduplicated.values())

    # Get execution date formatted as YYYYMMDD
    date = execution_date.strftime("%Y%m%d")

    # Define the file path for saving the JSON data
    path = r"/opt/airflow/data/raw/news/crawl_news-" + f"{date}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    write_json_atomic(news, path)

    # Print success message with total news items and file path
    print(f"The process of crawling {len(news)} unique news items was successful")
    print(f"Saving at {path}")
