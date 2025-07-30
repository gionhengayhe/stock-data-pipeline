from datetime import datetime

import requests
import json
import os
import requests

def crawl_companies(**kwargs):
    API_KEY = os.getenv("SEC_API_KEY")
    exchanges = ['NASDAQ', 'NYSE']

    list_companies = []
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchange}?token={API_KEY}'
        response = requests.get(url)
        data = response.json()
        list_companies.extend(data)
        print(f"Fetched {len(data)} companies from {exchange} exchange.")

    execution_date = kwargs['execution_date']
    date = execution_date.strftime('%Y%m%d')
    file_path = f'/opt/airflow/data/raw/companies/crawl_companies-{date}.json'
    json_object = json.dumps(list_companies, indent=4)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Write the JSON data to a file
    with open(file_path, "w") as outfile:
        outfile.write(json_object)
    print(f"The process of crawling {len(list_companies)} companies was successful")
    print(f"Saving at {file_path}")
