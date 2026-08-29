import os
from pathlib import Path

import pandas as pd
import requests


ENDPOINTS = (
    "dim_time",
    "dim_news",
    "dim_topics",
    "dim_companies",
    "fact_news_companies",
    "fact_news_topics",
    "fact_candles",
)
BASE_URL = os.getenv("API_BASE_URL", "http://localhost:5000").rstrip("/")
OUTPUT_PATH = Path(os.getenv("EXCEL_OUTPUT_PATH", "exported_data.xlsx"))
PAGE_SIZE = 5000


def fetch_table(session: requests.Session, table: str) -> pd.DataFrame:
    rows = []
    offset = 0
    while True:
        response = session.get(
            f"{BASE_URL}/{table}",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        page = response.json()
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return pd.DataFrame(rows)
        offset += PAGE_SIZE


def export_to_excel(output_path: Path = OUTPUT_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session, pd.ExcelWriter(
        output_path, engine="openpyxl"
    ) as writer:
        for table in ENDPOINTS:
            frame = fetch_table(session, table)
            frame.to_excel(writer, sheet_name=table, index=False)
            print(f"Exported {len(frame)} rows from {table}")
    print(f"Wrote dashboard extract to {output_path.resolve()}")


if __name__ == "__main__":
    export_to_excel()
