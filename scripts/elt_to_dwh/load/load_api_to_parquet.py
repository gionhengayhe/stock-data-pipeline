import os
import json
import polars as pl


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

def get_file_by_date(directory, execution_date, prefix, extension=".json"):
    """
    Get the file path by matching with the execution date and prefix.
    :param directory: Directory to search for files.
    :param execution_date: Execution date in the format YYYYMMDD.
    :param prefix: Prefix of the file name to search for.
    :param extension: File extension to look for, default is .json.
    :return: Full path to the matching file or None if not found.
    """
    target_filename = f"{prefix}-{execution_date}{extension}"
    target_path = os.path.join(directory, target_filename)
    print("Looking for file:", target_path)
    return target_path if os.path.exists(target_path) else None

def convert_ohlcs_to_parquet(**kwargs):
    input_directory = kwargs.get('input_directory', '/opt/airflow/data/raw/ohlcs')
    output_directory = kwargs.get('output_directory', '/opt/airflow/data/parquet/ohlcs')
    execution_date = kwargs["execution_date"].strftime("%Y%m%d")

    latest_file = get_file_by_date(input_directory, execution_date, "crawl_ohlcs")
    if latest_file:
        with open(latest_file, "r", encoding="utf-8") as stream:
            rows = json.load(stream)
        if not rows:
            # Empty OHLC is expected on weekends and market holidays.
            df = pl.DataFrame(schema=OHLC_SCHEMA)
        else:
            df = pl.DataFrame(rows)
            df = df.rename({
                "T": "ticker",
                "v": "volume",
                "vw": "volume_weighted",
                "o": "open",
                "c": "close",
                "h": "high",
                "l": "low",
                "t": "time_stamp",
                "n": "num_of_trades",
                "otc": "is_otc"
            })
            if "is_otc" not in df.columns:
                df = df.with_columns(pl.lit(False).alias("is_otc"))
            df = df.select(list(OHLC_SCHEMA)).cast(OHLC_SCHEMA)

        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
        os.makedirs(output_directory, exist_ok=True)
        df.write_parquet(output_filepath)
        print(f"[ohlcs] Saved Parquet file: {output_filepath}")
    else:
        raise FileNotFoundError(f"[ohlcs] Input file not found for {execution_date}")

def convert_news_to_parquet(**kwargs):
    input_directory = kwargs.get('input_directory', '/opt/airflow/data/raw/news')
    output_directory = kwargs.get('output_directory', '/opt/airflow/data/parquet/news')
    execution_date = kwargs["execution_date"].strftime("%Y%m%d")
    latest_file = get_file_by_date(input_directory, execution_date, "crawl_news")
    if latest_file:
        df = pl.read_json(latest_file)
        filename = os.path.basename(latest_file).replace('.json', ".parquet")
        output_filepath = os.path.join(output_directory, filename)
        os.makedirs(output_directory, exist_ok=True)
        df.write_parquet(output_filepath)
        print(f"[news] Saved Parquet file: {output_filepath}")
    else:
        raise FileNotFoundError(f"[news] Input file not found for {execution_date}")


