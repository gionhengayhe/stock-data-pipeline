import requests
import json
import datetime
import os
# Get the current time
# Format string as YYYYMMDDTHHMM
def get_data_by_time_range(execution_date, time_zone):
    """Get data based on the current time zone range."""
    if time_zone == 1:
        time_from = execution_date.strftime("%Y%m%dT" + "0000")
        time_to = execution_date.strftime("%Y%m%dT" + "2359")
    elif time_zone == 2:
        time_from = execution_date.strftime("%Y%m%dT" + "0000")
        time_to = execution_date.strftime("%Y%m%dT" + "1200")
    else:
        time_from = execution_date.strftime("%Y%m%dT" + "1201")
        time_to = execution_date.strftime("%Y%m%dT" + "2359")
    return time_from, time_to


def crawl_news(**kwargs):
    execution_date = kwargs["execution_date"]
    limit = "1000"
    apikey = os.getenv("ALPHA_VANTAGE_API_KEY")

    json_object = []
    total = 0
    for time_zone in [1, 2, 3]:
        # Call the function to get data
        time_from, time_to = get_data_by_time_range(execution_date, time_zone)
        print(f"Get news from {time_from} to {time_to}")

        # Construct the API URL with query parameters
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={apikey}'

        # Make a GET request to the API
        r = requests.get(url)
        response = r.json()
        if "feed" not in response:
            raise KeyError(f"'feed' key not found in response. Full response:\n{json.dumps(response, indent=4)}")
        data = response["feed"]

        # Increment the total count of news items
        total += len(data)

        if total == 1000 and time_zone == 1:
            continue

        # Append the data to the json_object list
        json_object += data

        if total < 1000 and time_zone == 1:
            break

    # Serialize the JSON object to a formatted string
    json_object = json.dumps(json_object, indent=4)

    # Get execution date formatted as YYYYMMDD
    date = execution_date.strftime("%Y%m%d")

    # Define the file path for saving the JSON data
    path = r"/opt/airflow/data/raw/news/crawl_news-" + f"{date}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Write the JSON data to a file
    with open(path, "w") as outfile:
        outfile.write(json_object)

    # Print success message with total news items and file path
    print(f"The process of crawling {total} news was successful")
    print(f"Saving at {path}")