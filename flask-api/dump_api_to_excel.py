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
OUTPUT_PATH = (
    Path(__file__).resolve().parent
    / "exported_data"
    / "stock_data.xlsx"
)
PAGE_SIZE = 5000
MAX_EXPORT_ATTEMPTS = 3


class SnapshotChangedError(RuntimeError):
    """Raised when the warehouse changes during a multi-request export."""


def fetch_snapshot(session: requests.Session) -> dict[str, str | None]:
    response = session.get(f"{BASE_URL}/snapshot", timeout=30)
    response.raise_for_status()
    snapshot = response.json()
    if not snapshot.get("snapshot_token"):
        raise RuntimeError("API /snapshot did not return a snapshot_token")
    return snapshot


def fetch_table(
    session: requests.Session, table: str, snapshot_token: str
) -> pd.DataFrame:
    rows = []
    offset = 0
    while True:
        response = session.get(
            f"{BASE_URL}/{table}",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "snapshot": snapshot_token,
            },
            timeout=30,
        )
        if response.status_code == 409:
            raise SnapshotChangedError(
                "Warehouse changed while API pages were being fetched"
            )
        response.raise_for_status()
        if response.headers.get("X-Data-Snapshot") != snapshot_token:
            raise SnapshotChangedError(
                f"API returned a different snapshot for {table}"
            )
        page = response.json()
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return pd.DataFrame(rows)
        offset += PAGE_SIZE


def fetch_consistent_tables(
    session: requests.Session,
) -> dict[str, pd.DataFrame]:
    for attempt in range(1, MAX_EXPORT_ATTEMPTS + 1):
        snapshot = fetch_snapshot(session)
        snapshot_token = str(snapshot["snapshot_token"])
        try:
            tables = {}
            for table in ENDPOINTS:
                tables[table] = fetch_table(session, table, snapshot_token)
                print(f"Fetched {len(tables[table])} rows from {table}")

            final_snapshot = fetch_snapshot(session)
            if final_snapshot["snapshot_token"] != snapshot_token:
                raise SnapshotChangedError(
                    "Warehouse changed after the final API page was fetched"
                )
            return tables
        except SnapshotChangedError as exc:
            if attempt == MAX_EXPORT_ATTEMPTS:
                raise RuntimeError(
                    f"Could not obtain one consistent warehouse snapshot after "
                    f"{MAX_EXPORT_ATTEMPTS} attempts"
                ) from exc
            print(f"Snapshot changed; restarting export attempt {attempt + 1}")

    raise AssertionError("unreachable")


def export_to_excel() -> Path:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        tables = fetch_consistent_tables(session)

    temporary_path = OUTPUT_PATH.with_name(
        f".{OUTPUT_PATH.stem}.tmp{OUTPUT_PATH.suffix}"
    )
    try:
        with pd.ExcelWriter(temporary_path, engine="openpyxl") as writer:
            for table, frame in tables.items():
                frame.to_excel(writer, sheet_name=table, index=False)
                print(f"Exported {len(frame)} rows from {table}")
        os.replace(temporary_path, OUTPUT_PATH)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f"Wrote raw dimension and fact tables to {OUTPUT_PATH}")
    return OUTPUT_PATH


if __name__ == "__main__":
    export_to_excel()
