from scripts.common.config import DATA_ROOT, required_env
from scripts.common.files import dated_file, write_json_atomic
from scripts.common.http import get_json

def crawl_markets(**kwargs):
    api_key = required_env("ALPHA_VANTAGE_API_KEY")
    response = get_json(
        'https://www.alphavantage.co/query',
        params={"function": "MARKET_STATUS", "apikey": api_key},
        required_key="markets",
        force_ipv6=True,
    )
    data = response["markets"]

    file_path = dated_file(
        DATA_ROOT / "raw" / "markets",
        "crawl_markets",
        kwargs["execution_date"],
        ".json",
    )
    write_json_atomic(data, file_path)
    print(f"The process of crawling {len(data)} regions and exchanges was successful.")
    print(f"Saving at {file_path}")
