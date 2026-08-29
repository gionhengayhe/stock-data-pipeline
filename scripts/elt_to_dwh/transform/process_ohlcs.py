import duckdb
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, from_unixtime, lit

from scripts.common.config import duckdb_path
from scripts.common.spark import (
    create_spark_session,
    execution_date_argument,
    s3a_parquet_uri,
)
from scripts.common.warehouse import ensure_time, register_spark_frame, transaction


def transform_ohlcs(
    candles: DataFrame, companies: DataFrame, time_id: int
) -> DataFrame:
    return (
        candles.join(broadcast(companies), on="ticker", how="inner")
        .filter(col("volume_weighted").isNotNull())
        .withColumn("time_id", lit(time_id))
        .withColumn(
            "time_stamp",
            from_unixtime(col("time_stamp") / 1000).cast("timestamp"),
        )
        .fillna({"is_otc": False})
        .select(
            "company_id",
            "time_id",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "volume_weighted",
            "time_stamp",
            "num_of_trades",
            "is_otc",
        )
        .dropDuplicates(["company_id", "time_id"])
    )


def process_ohlcs() -> None:
    execution_date = execution_date_argument()
    spark = create_spark_session("process_ohlcs")
    try:
        source = spark.read.parquet(s3a_parquet_uri("ohlcs", execution_date))
        if source.isEmpty():
            print(f"No OHLC data for {execution_date.date()}; market may be closed")
            return

        with duckdb.connect(duckdb_path()) as conn, transaction(conn):
            time_id = ensure_time(conn, execution_date)
            company_rows = conn.execute(
                """
                WITH existing_mapping AS (
                    SELECT DISTINCT c.id AS company_id, c.ticker
                    FROM fact_candles AS fact
                    JOIN dim_companies AS c ON c.id = fact.company_id
                    WHERE fact.time_id = ?
                ),
                canonical_mapping AS (
                    SELECT id AS company_id, ticker
                    FROM (
                        SELECT id, ticker,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ticker
                                   ORDER BY is_delisted ASC,
                                            updated_time DESC NULLS LAST,
                                            id DESC
                               ) AS row_num
                        FROM dim_companies
                    )
                    WHERE row_num = 1
                )
                SELECT company_id, ticker FROM existing_mapping
                UNION ALL
                SELECT company_id, ticker FROM canonical_mapping AS canonical
                WHERE NOT EXISTS (
                    SELECT 1 FROM existing_mapping AS existing
                    WHERE existing.ticker = canonical.ticker
                )
                """,
                [time_id],
            ).fetchdf()
            companies = spark.createDataFrame(company_rows)
            candles = transform_ohlcs(source, companies, time_id)
            row_count = register_spark_frame(conn, "incoming_candles", candles)
            conn.execute(
                """
                INSERT INTO fact_candles (
                    company_id, time_id, open, high, low, close, volume,
                    volume_weighted, time_stamp, num_of_trades, is_otc
                )
                SELECT company_id, time_id, open, high, low, close, volume,
                       volume_weighted, time_stamp, num_of_trades, is_otc
                FROM incoming_candles
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
        print(f"Loaded {row_count} OHLC rows into DuckDB")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_ohlcs()
