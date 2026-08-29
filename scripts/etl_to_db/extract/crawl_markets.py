import os

from scripts.common.files import write_json_atomic
from scripts.common.http import get_json

def crawl_markets(**kwargs):
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is required")
    response = get_json(
        'https://www.alphavantage.co/query',
        params={"function": "MARKET_STATUS", "apikey": api_key},
        required_key="markets",
    )
    data = response["markets"]

    execution_date = kwargs['execution_date']
    date = execution_date.strftime('%Y%m%d')
    file_path = f'/opt/airflow/data/raw/markets/crawl_markets-{date}.json'
    write_json_atomic(data, file_path)
    print(f"The process of crawling {len(data)} regions and exchanges was successful.")
    print(f"Saving at {file_path}")
