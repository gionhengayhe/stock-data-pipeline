import polars as pl

from scripts.common.config import DATA_ROOT
from scripts.common.files import dated_file, read_json


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


def _paths(dataset: str, execution_date, input_directory=None, output_directory=None):
    raw_directory = input_directory or DATA_ROOT / "raw" / dataset
    parquet_directory = output_directory or DATA_ROOT / "parquet" / dataset
    source = dated_file(
        raw_directory,
        f"crawl_{dataset}",
        execution_date,
        ".json",
    )
    target = dated_file(
        parquet_directory,
        f"crawl_{dataset}",
        execution_date,
        ".parquet",
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    return source, target


def convert_ohlcs_to_parquet(**context) -> None:
    source, target = _paths(
        "ohlcs",
        context["execution_date"],
        context.get("input_directory"),
        context.get("output_directory"),
    )
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
    print(f"Wrote {frame.height} OHLC rows to {target}")


def convert_news_to_parquet(**context) -> None:
    source, target = _paths(
        "news",
        context["execution_date"],
        context.get("input_directory"),
        context.get("output_directory"),
    )
    frame = pl.read_json(source)
    frame.write_parquet(target)
    print(f"Wrote {frame.height} news rows to {target}")
