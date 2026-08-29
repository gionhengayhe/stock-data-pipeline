from scripts.common.config import DATA_ROOT, required_env
from scripts.common.files import dated_file, write_json_atomic
from scripts.common.http import get_json

def crawl_companies(**kwargs):
    api_key = required_env("SEC_API_KEY")
    exchanges = ['NASDAQ', 'NYSE']

    list_companies = []
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchange}'
        data = get_json(url, params={"token": api_key})
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected company payload for exchange {exchange}")
        list_companies.extend(data)
        print(f"Fetched {len(data)} companies from {exchange} exchange.")

    file_path = dated_file(
        DATA_ROOT / "raw" / "companies",
        "crawl_companies",
        kwargs["execution_date"],
        ".json",
    )
    write_json_atomic(list_companies, file_path)
    print(f"The process of crawling {len(list_companies)} companies was successful")
    print(f"Saving at {file_path}")
