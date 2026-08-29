import os

from scripts.common.files import write_json_atomic
from scripts.common.http import get_json


def crawl_ohlcs(**kwargs):
    execution_date = kwargs["execution_date"]
    date_crawl = execution_date.strftime("%Y-%m-%d")

    # API key for authentication
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY is required")

    # Set parameters for the API request
    adjusted = "true"
    include_otc = "true"

    # Construct the API URL with query parameters
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}'
    payload = get_json(
        url,
        params={"adjusted": adjusted, "include_otc": include_otc, "apiKey": api_key},
    )
    if payload.get("status") not in {None, "OK", "DELAYED"}:
        raise RuntimeError(f"Polygon returned status {payload.get('status')}")
    data = payload.get("results", [])

    # Get execution date formatted as YYYYMMDD
    date = execution_date.strftime("%Y%m%d")

    # Define the file path for saving the JSON data
    path = r"/opt/airflow/data/raw/ohlcs/crawl_ohlcs-" + f"{date}.json"
    write_json_atomic(data, path)

    # Print success message with total OHLCs and file path
    print(f"The process of crawling {len(data)} OHLCs was successful")
    print(f"Saving at {path}")
