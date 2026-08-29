import os

from scripts.common.files import write_json_atomic
from scripts.common.http import get_json

def crawl_companies(**kwargs):
    api_key = os.getenv("SEC_API_KEY")
    if not api_key:
        raise RuntimeError("SEC_API_KEY is required")
    exchanges = ['NASDAQ', 'NYSE']

    list_companies = []
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchange}'
        data = get_json(url, params={"token": api_key})
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected company payload for exchange {exchange}")
        list_companies.extend(data)
        print(f"Fetched {len(data)} companies from {exchange} exchange.")

    execution_date = kwargs['execution_date']
    date = execution_date.strftime('%Y%m%d')
    file_path = f'/opt/airflow/data/raw/companies/crawl_companies-{date}.json'
    write_json_atomic(list_companies, file_path)
    print(f"The process of crawling {len(list_companies)} companies was successful")
    print(f"Saving at {file_path}")
