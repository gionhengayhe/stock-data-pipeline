from pathlib import Path

import polars as pl
import psycopg2

from scripts.common.config import DATA_ROOT, DATABASE_ROOT, postgres_config
from scripts.common.files import dated_file, read_json
from scripts.common.storage import upload_parquet


OHLC_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.Int64,
    "volume_weighted": pl.Float64,
    "open": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "time_stamp": pl.Int64,
    "num_of_trades": pl.Int64,
    "is_otc": pl.Boolean,
}


def _artifact_path(
    layer: str,
    dataset: str,
    execution_date,
    suffix: str,
    root: str | Path = DATA_ROOT,
) -> Path:
    return dated_file(
        Path(root) / layer / dataset,
        f"crawl_{dataset}",
        execution_date,
        suffix,
    )


def _parquet_target(dataset: str, execution_date, output_root=None) -> Path:
    target = _artifact_path(
        "parquet",
        dataset,
        execution_date,
        ".parquet",
        output_root or DATA_ROOT,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def convert_ohlcs_to_parquet(**context) -> None:
    execution_date = context["execution_date"]
    input_root = context.get("input_root") or DATA_ROOT
    output_root = context.get("output_root") or DATA_ROOT
    source = _artifact_path(
        "raw", "ohlcs", execution_date, ".json", input_root
    )
    target = _parquet_target("ohlcs", execution_date, output_root)

    rows = read_json(source)
    if not rows:
        frame = pl.DataFrame(schema=OHLC_SCHEMA)
    else:
        frame = pl.DataFrame(rows).rename(
            {
                "T": "ticker",
                "v": "volume",
                "vw": "volume_weighted",
                "o": "open",
                "c": "close",
                "h": "high",
                "l": "low",
                "t": "time_stamp",
                "n": "num_of_trades",
                "otc": "is_otc",
            },
            strict=False,
        )
        if "is_otc" not in frame.columns:
            frame = frame.with_columns(pl.lit(False).alias("is_otc"))
        frame = frame.select(list(OHLC_SCHEMA)).cast(OHLC_SCHEMA)

    frame.write_parquet(target)
    print(f"Staged {frame.height} OHLC rows at {target}")


def convert_news_to_parquet(**context) -> None:
    execution_date = context["execution_date"]
    input_root = context.get("input_root") or DATA_ROOT
    output_root = context.get("output_root") or DATA_ROOT
    source = _artifact_path(
        "raw", "news", execution_date, ".json", input_root
    )
    target = _parquet_target("news", execution_date, output_root)

    frame = pl.read_json(source)
    frame.write_parquet(target)
    print(f"Staged {frame.height} news rows at {target}")


def extract_companies_to_parquet(**context) -> None:
    query_path = DATABASE_ROOT / "config_db" / "extract_company.sql"
    query = query_path.read_text(encoding="utf-8")
    with psycopg2.connect(**postgres_config()) as connection:
        frame = pl.read_database(query=query, connection=connection)

    target = _parquet_target("companies", context["execution_date"])
    frame.write_parquet(target)
    print(f"Staged {frame.height} company rows at {target}")


def upload_daily_artifacts(**context) -> None:
    execution_date = context["execution_date"]
    for dataset in ("companies", "news", "ohlcs"):
        path = _parquet_target(dataset, execution_date)
        if not path.exists():
            raise FileNotFoundError(
                f"Expected staged artifact does not exist: {path}"
            )
        upload_parquet(path, dataset, execution_date)
