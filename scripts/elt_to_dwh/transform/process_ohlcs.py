import os
import sys
from datetime import datetime

import boto3
import duckdb
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit

from scripts.common.config import duckdb_path


def get_spark_session():
    """Create a Spark session configured for S3 access."""
    return (
        SparkSession.builder.appName("process_ohlcs")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .getOrCreate()
    )


def get_parquet_path_by_date(bucket: str, prefix: str, execution_date: str) -> str:
    target_key = f"{prefix}/crawl_{prefix}-{execution_date}.parquet"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=target_key)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"] == target_key]
    if not files:
        raise FileNotFoundError(f"No Parquet file found for {execution_date} at prefix {prefix}")
    return f"s3a://{bucket}/{files[0]}"


def process_ohlcs():
    execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise RuntimeError("BUCKET_NAME is required")
    parquet_path = get_parquet_path_by_date(
        bucket, "ohlcs", execution_date.strftime("%Y%m%d")
    )
    print("Reading OHLC Parquet for:", execution_date.date())

    spark = get_spark_session()
    try:
        df = spark.read.parquet(parquet_path)
        if df.rdd.isEmpty():
            print(f"No OHLC data for {execution_date.date()}; market may be closed.")
            return

        with duckdb.connect(duckdb_path()) as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    """
                    INSERT INTO dim_time (date, day_of_week, month, quarter, year)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (date) DO NOTHING
                    """,
                    [
                        execution_date.date(),
                        execution_date.strftime("%A"),
                        execution_date.strftime("%B"),
                        (execution_date.month - 1) // 3 + 1,
                        execution_date.year,
                    ],
                )
                time_id = int(
                    conn.execute(
                        "SELECT id FROM dim_time WHERE date = ?", [execution_date.date()]
                    ).fetchone()[0]
                )
                companies = spark.createDataFrame(
                    conn.execute("SELECT id, ticker FROM dim_companies").fetchdf()
                ).dropDuplicates(["ticker"])

                transformed = (
                    df.join(companies, on="ticker", how="left")
                    .filter(col("id").isNotNull() & col("volume_weighted").isNotNull())
                    .withColumnRenamed("id", "company_id")
                    .withColumn("time_id", lit(time_id))
                    .withColumn(
                        "time_stamp",
                        from_unixtime(col("time_stamp") / 1000).cast("timestamp"),
                    )
                )
                conn.register("temp_candles", pl.from_pandas(transformed.toPandas()).to_arrow())
                conn.execute(
                    """
                    INSERT INTO fact_candles (
                        company_id, time_id, open, high, low, close, volume,
                        volume_weighted, time_stamp, num_of_trades, is_otc
                    )
                    SELECT company_id, time_id, open, high, low, close, volume,
                           volume_weighted, time_stamp, num_of_trades, is_otc
                    FROM temp_candles
                    ON CONFLICT (company_id, time_id) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        volume_weighted = EXCLUDED.volume_weighted,
                        time_stamp = EXCLUDED.time_stamp,
                        num_of_trades = EXCLUDED.num_of_trades,
                        is_otc = EXCLUDED.is_otc
                    """
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        print("OHLC data loaded successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_ohlcs()
