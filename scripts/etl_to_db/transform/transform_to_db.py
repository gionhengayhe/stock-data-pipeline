import os
import json
from datetime import datetime
import re

import polars as pl


def get_file_by_date(directory: str, date_str: str) -> str:
    """
    Get the file path by matching with the execution date.

    :param directory: Directory to search for files.
    :param date_str: Date string in the format YYYYMMDD.
    :return: Full path to the matching file.
    """
    pattern = re.compile(rf".*-{date_str}\.json$")
    for f in os.listdir(directory):
        if pattern.match(f):
            return os.path.join(directory, f)
    raise FileNotFoundError(f"No file found for date {date_str} in {directory}")

def read_file(directory, date_str: str):
    """
    Read the latest JSON file in a directory.

    :param directory: Directory to search for files.
    :return: Data from the JSON file or an empty list if no file is found.
    """
    latest_file = get_file_by_date(directory, date_str)
    if latest_file:
        with open(latest_file, 'r') as file:
            data = json.load(file)
        return data
    else:
        print(f"No JSON files found in {directory}.")
        return []


def cleaned_data(data):
    return data.drop_nulls().unique()

def df_to_json(df, file_path):
    """
    Save a DataFrame to a JSON file.

    :param df: The DataFrame to save.
    :param file_path: The path where the JSON file will be saved.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.write_json(file_path)
    print(f"Data saved to {file_path}")


def transform_to_db(**kwargs):
    execution_date = kwargs['execution_date']
    date = execution_date.strftime("%Y%m%d")

    companies = read_file('/opt/airflow/data/raw/companies', date)
    markets = read_file('/opt/airflow/data/raw/markets', date)
    #Transform regions
    regions_schema = [
        {
            "region": item["region"],
            "local_open": item["local_open"],
            "local_close": item["local_close"],
        } for item in markets
    ]
    regions_df = cleaned_data(pl.DataFrame(regions_schema))
    regions_path = f'/opt/airflow/data/processed/regions/regions-{date}.json'
    df_to_json(regions_df, regions_path)

    #Transform industries
    industries_schema = [
        {
            "industry": item["industry"],
            "sector": item["sector"],
        } for item in companies
    ]
    industries_df = cleaned_data(pl.DataFrame(industries_schema))
    industries_path = f'/opt/airflow/data/processed/industries/industries-{date}.json'
    df_to_json(industries_df, industries_path)

    # Transform SIC industries
    sic_industries_schema = [
        {
            "id": item["sic"],
            "sic_industry": item["sicIndustry"],
            "sic_sector": item["sicSector"],
        } for item in companies if "sic" in item
    ]
    sic_industries_df = cleaned_data(pl.DataFrame(sic_industries_schema))
    sic_industries_path = f'/opt/airflow/data/processed/sic_industries/sic_industries-{date}.json'
    df_to_json(sic_industries_df, sic_industries_path)

    # Transform exchanges
    exchanges_schema = [
        {
            "name": exchange.strip(),
            "region": item["region"],
        }
        for item in markets
        for exchange in item["primary_exchanges"].split(",")
        if item.get("primary_exchanges")
    ]
    exchanges_df = cleaned_data(pl.DataFrame(exchanges_schema))
    exchanges_path = f'/opt/airflow/data/processed/exchanges/exchanges-{date}.json'
    df_to_json(exchanges_df, exchanges_path)

    # Transform companies
    companies_schema = [
        {
            "name": item["name"],
            "ticker": item["ticker"],
            "is_delisted": item["isDelisted"],
            "category": item["category"],
            "currency": item["currency"],
            "location": item["location"],
            "industry": item["industry"],
            "sector": item["sector"],
            "exchange": item["exchange"],
            "sic_industry": item["sicIndustry"],
            "sic_sector": item["sicSector"],
        } for item in companies
    ]

    companies_df = cleaned_data(pl.DataFrame(companies_schema))
    # Filter companies based on exchange and currency
    companies_df = companies_df.filter(
        (pl.col("exchange").is_in(["NASDAQ", "NYSE"])) &
        (pl.col("currency") == "USD")
    )
    companies_path = f'/opt/airflow/data/processed/companies/companies-{date}.json'
    df_to_json(companies_df, companies_path)
if __name__ == "__main__":
    transform_to_db()
    print("Transformation and save to json file completed.")
