import requests
import json
import datetime
import os
def crawl_ohlcs(**kwargs):
    execution_date = kwargs["execution_date"]
    date_crawl = execution_date.strftime("%Y-%m-%d")

    # API key for authentication
    apiKey = os.getenv("POLYGON_API_KEY")

    # Set parameters for the API request
    adjusted = "true"
    include_otc = "true"

    # Construct the API URL with query parameters
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_crawl}?adjusted={adjusted}&include_otc={include_otc}&apiKey={apiKey}'
    print("url:", url)
    # Make a GET request to the API
    r = requests.get(url)

    # Parse the response JSON
    data = r.json()
    data = data.get("results", [])

    # Serialize the JSON object to a formatted string
    json_object = json.dumps(data, indent=4)

    # Get execution date formatted as YYYYMMDD
    date = execution_date.strftime("%Y%m%d")

    # Define the file path for saving the JSON data
    path = r"/opt/airflow/data/raw/ohlcs/crawl_ohlcs-" + f"{date}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)


    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)

    # Print success message with total OHLCs and file path
    print(f"The process of crawling {len(data)} OHLCs was successful")
    print(f"Saving at {path}")
