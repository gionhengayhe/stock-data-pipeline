import os
import sys
from datetime import datetime

import boto3
import duckdb
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_timestamp

from scripts.common.config import duckdb_path


def get_spark_session():
    return (
        SparkSession.builder.appName("process_news")
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


def _latest_company_ids(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    return pl.from_pandas(
        conn.execute(
            """
            SELECT id, ticker
            FROM (
                SELECT id, ticker, updated_time,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY updated_time DESC, id DESC
                       ) AS row_num
                FROM dim_companies
            )
            WHERE row_num = 1
            """
        ).fetchdf()
    ).rename({"id": "company_id"})


def process_news():
    execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise RuntimeError("BUCKET_NAME is required")
    parquet_path = get_parquet_path_by_date(
        bucket, "news", execution_date.strftime("%Y%m%d")
    )
    print("Reading news Parquet for:", execution_date.date())

    spark = get_spark_session()
    try:
        df = spark.read.parquet(parquet_path)
        if df.rdd.isEmpty():
            print(f"No news data for {execution_date.date()}.")
            return

        with duckdb.connect(duckdb_path()) as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                topics = (
                    df.select(explode(col("topics")).alias("topic"))
                    .select("topic.topic")
                    .distinct()
                    .withColumnRenamed("topic", "name")
                    .toPandas()
                )
                conn.register("temp_topics", topics)
                conn.execute(
                    """
                    INSERT INTO dim_topics (name)
                    SELECT name FROM temp_topics
                    ON CONFLICT (name) DO NOTHING
                    """
                )

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

                news_df = df.withColumn("time_id", lit(time_id)).withColumn(
                    "time_published",
                    to_timestamp(col("time_published"), "yyyyMMdd'T'HHmmss"),
                )
                conn.register("temp_news", news_df.toPandas())
                conn.execute(
                    """
                    INSERT INTO dim_news (
                        title, url, time_published, authors, summary, source,
                        overall_sentiment_score, overall_sentiment_label, time_id
                    )
                    SELECT title, url, time_published, authors, summary, source,
                           overall_sentiment_score, overall_sentiment_label, time_id
                    FROM temp_news
                    ON CONFLICT (url) DO UPDATE SET
                        title = EXCLUDED.title,
                        time_published = EXCLUDED.time_published,
                        authors = EXCLUDED.authors,
                        summary = EXCLUDED.summary,
                        source = EXCLUDED.source,
                        overall_sentiment_score = EXCLUDED.overall_sentiment_score,
                        overall_sentiment_label = EXCLUDED.overall_sentiment_label,
                        time_id = EXCLUDED.time_id
                    """
                )

                news_ids = pl.from_pandas(
                    conn.execute("SELECT id, url FROM dim_news").fetchdf()
                ).rename({"id": "new_id"})
                topic_ids = pl.from_pandas(
                    conn.execute("SELECT id, name FROM dim_topics").fetchdf()
                ).rename({"id": "topic_id"})

                news_topics = pl.from_pandas(
                    df.select(explode(col("topics")).alias("topic"), col("url"))
                    .select(
                        col("topic.relevance_score").alias("relevance_score"),
                        col("topic.topic").alias("name"),
                        col("url"),
                    )
                    .toPandas()
                )
                news_topics = news_topics.join(topic_ids, on="name", how="inner").join(
                    news_ids, on="url", how="inner"
                )
                conn.register("temp_news_topics", news_topics.to_arrow())
                conn.execute(
                    """
                    INSERT INTO fact_news_topics (new_id, topic_id, relevance_score)
                    SELECT new_id, topic_id, relevance_score FROM temp_news_topics
                    ON CONFLICT (new_id, topic_id) DO UPDATE SET
                        relevance_score = EXCLUDED.relevance_score
                    """
                )

                news_companies = pl.from_pandas(
                    df.select(
                        explode(col("ticker_sentiment")).alias("ticker_sentiment"),
                        col("url"),
                    )
                    .select(
                        col("ticker_sentiment.ticker").alias("ticker"),
                        col("ticker_sentiment.ticker_sentiment_score").alias(
                            "ticker_sentiment_score"
                        ),
                        col("ticker_sentiment.ticker_sentiment_label").alias(
                            "ticker_sentiment_label"
                        ),
                        col("ticker_sentiment.relevance_score").alias("relevance_score"),
                        col("url"),
                    )
                    .toPandas()
                )
                news_companies = news_companies.join(
                    _latest_company_ids(conn), on="ticker", how="inner"
                ).join(news_ids, on="url", how="inner")
                conn.register("temp_news_companies", news_companies.to_arrow())
                conn.execute(
                    """
                    INSERT INTO fact_news_companies (
                        new_id, company_id, ticker_sentiment_score,
                        ticker_sentiment_label, relevance_score
                    )
                    SELECT new_id, company_id, ticker_sentiment_score,
                           ticker_sentiment_label, relevance_score
                    FROM temp_news_companies
                    ON CONFLICT (new_id, company_id) DO UPDATE SET
                        ticker_sentiment_score = EXCLUDED.ticker_sentiment_score,
                        ticker_sentiment_label = EXCLUDED.ticker_sentiment_label,
                        relevance_score = EXCLUDED.relevance_score
                    """
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        print("News data loaded successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    process_news()
