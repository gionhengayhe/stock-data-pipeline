import requests
import json
from datetime import datetime
import os

def crawl_markets(**kwargs):
    API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = f'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey={API_KEY}'
    r = requests.get(url)
    response = r.json()
    if "markets" not in response:
        raise KeyError(f"'markets' key not found in response. Full response:\n{json.dumps(response, indent=4)}")
    data = response["markets"]

    execution_date = kwargs['execution_date']
    date = execution_date.strftime('%Y%m%d')
    file_path = f'/opt/airflow/data/raw/markets/crawl_markets-{date}.json'
    json_object = json.dumps(data, indent=4)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as outfile:
        outfile.write(json_object)
    print(f"The process of crawling {len(data)} regions and exchanges was successful.")
    print(f"Saving at {file_path}")