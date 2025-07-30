import polars as pl
import duckdb
import boto3
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col


def get_spark_session():
    spark = SparkSession.builder \
        .appName("process_news") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    return spark

def get_parquet_path_by_date(bucket: str, prefix: str, execution_date: str) -> str:
    target_key = f"{prefix}/crawl_{prefix}-{execution_date}.parquet"
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=target_key)
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"] == target_key]

    if not files:
        raise FileNotFoundError(f"No Parquet file found for {execution_date} at prefix {prefix}")

    return f"s3a://{bucket}/{files[0]}"

def process_news():
    #Get the execution date from command line arguments
    execution_date_str = sys.argv[1]
    print("Execution date string:", execution_date_str)
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")

    bucket = os.getenv("BUCKET_NAME")
    prefix = "news"
    parquet_path = get_parquet_path_by_date(bucket, prefix, execution_date.strftime("%Y%m%d"))
    print("Reading from:", parquet_path)

    spark = get_spark_session()

    df = spark.read.parquet(parquet_path)
    print("DataFrame loaded from Parquet:")
    df.printSchema()
    df.show(5)
    conn = duckdb.connect("/opt/airflow/database/config_dwh/mydb.duckdb")

    # Step 1: Create DataFrame for dim_topics and insert new topics if they do not exist
    df_topics = df.select(explode(col("topics")).alias("topic")) \
        .select("topic.topic").distinct().withColumnRenamed("topic", "name")
    print("Topics DataFrame:")
    df_topics.show()
    arrow_topics = df_topics.toPandas()
    conn.register("temp_topics", arrow_topics)
    conn.execute("""
        INSERT INTO dim_topics (name)
        SELECT name FROM temp_topics
        WHERE name NOT IN (SELECT name FROM dim_topics)
    """)
    print("Data inserted into dim_topics successfully.")

    #Step 2: Add execution date to dim_time if it does not exist
    conn.execute(f"""
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT
                '{execution_date}',
                '{execution_date.strftime('%A')}',
                '{execution_date.strftime('%B')}',
                '{(execution_date.month - 1) // 3 + 1}',
                '{execution_date.year}'
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{execution_date}'
        )
        """)
    print("Data inserted into dim_time successfully.")

    # Get the time_id for the execution date
    id_time_df = conn.execute(f'''
                SELECT id FROM dim_time WHERE date = '{execution_date}'
            ''').fetchdf()
    print("Time ID DataFrame:")
    print(id_time_df)
    news_time_id = id_time_df['id'][0]

    #Step 3: Create DataFrame for dim_news and insert news if they do not exist
    df_news = df.withColumn("time_id", lit(news_time_id))
    print("Dim_news DataFrame:")
    df_news.show(5)
    temp_news = df_news.toPandas()
    conn.register("temp_news", temp_news)
    conn.execute("""
        INSERT INTO dim_news (title, url, time_published, authors, summary, source, overall_sentiment_score, overall_sentiment_label, time_id)
        SELECT 
            title, url, time_published, authors, summary, source, overall_sentiment_score, overall_sentiment_label, time_id
        FROM temp_news
        WHERE (title, url, time_published) NOT IN (
            SELECT title, url, time_published FROM dim_news
        )
    """)
    print("Data inserted into dim_news successfully!")

    #Step 4: Create DataFrame for fact_news_topics and insert news topics if they do not exist
    df_news_topics = df.select(
        explode(col("topics")).alias("topic"),
        col("title"),
        col("url")
    ).select(
        col("topic.relevance_score").alias("relevance_score"),
        col("topic.topic").alias("name"),
        col("title"),
        col("url")
    )
    temp_news_topics = pl.from_pandas(df_news_topics.toPandas())

    #Get topic_id for fact_news_topics
    id_topic_df = pl.from_pandas(conn.execute("""
        SELECT id, name FROM dim_topics
    """).fetchdf())
    id_topic_df = id_topic_df.rename({"id":"topic_id"})
    temp_news_topics = temp_news_topics.join(id_topic_df, on="name", how="left")
    temp_news_topics = temp_news_topics.filter(pl.col("topic_id").is_not_null())

    #Get new_id for fact_news_topics
    id_news_df = pl.from_pandas(conn.execute("""
        SELECT id, title, url FROM dim_news
    """).fetchdf())
    id_news_df = id_news_df.rename({"id":"new_id"})
    temp_news_topics = temp_news_topics.join(id_news_df, on=["title","url"], how="left")
    temp_news_topics = temp_news_topics.filter(pl.col("new_id").is_not_null())
    print("DataFrame for fact_news_topics:")
    print(temp_news_topics)

    #Load data into fact_news_topics
    conn.register("temp_news_topics", temp_news_topics.to_arrow())
    conn.execute("""
        INSERT INTO fact_news_topics (new_id, topic_id, relevance_score)
        SELECT 
            new_id, topic_id, relevance_score
        FROM temp_news_topics
        WHERE (new_id, topic_id) NOT IN (
            SELECT new_id, topic_id FROM fact_news_topics
        )
    """)
    print("Data inserted into fact_news_topics successfully!")

    #Step 5: Create DataFrame for fact_news_companies and insert news companies if they do not exist
    df_news_companies = df.select(
        explode(col("ticker_sentiment")).alias("ticker_sentiment"),
        col("title"),
        col("url")
    ).select(
        col("ticker_sentiment.ticker").alias("ticker"),
        col("ticker_sentiment.ticker_sentiment_score").alias("ticker_sentiment_score"),
        col("ticker_sentiment.ticker_sentiment_label").alias("ticker_sentiment_label"),
        col("ticker_sentiment.relevance_score").alias("relevance_score"),
        col("title"),
        col("url")
    )
    temp_news_companies = pl.from_pandas(df_news_companies.toPandas())
    #Get company_id for fact_news_companies
    id_company_df = pl.from_pandas(conn.execute("""
        SELECT id, ticker, updated_time
        FROM (
            SELECT 
                id, 
                ticker, 
                updated_time,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY updated_time DESC) as row_num
            FROM dim_companies
        ) subquery
        WHERE row_num = 1;
    """).fetchdf())
    id_company_df = id_company_df.rename({"id":"company_id"})
    temp_news_companies = temp_news_companies.join(id_company_df, on="ticker", how="left")
    temp_news_companies = temp_news_companies.filter(pl.col("company_id").is_not_null())
    #Get new_id for fact_news_companies
    temp_news_companies = temp_news_companies.join(id_news_df, on=["title","url"], how="left")
    temp_news_companies = temp_news_companies.filter(pl.col("new_id").is_not_null())
    #Load data into fact_news_companies
    print("DataFrame for fact_news_companies:")
    print(temp_news_companies)
    conn.register("temp_news_companies", temp_news_companies.to_arrow())
    conn.execute("""
        INSERT INTO fact_news_companies (new_id, company_id, ticker_sentiment_score, ticker_sentiment_label, relevance_score)
        SELECT 
            new_id, company_id, ticker_sentiment_score, ticker_sentiment_label, relevance_score
        FROM temp_news_companies
        WHERE (new_id, company_id) NOT IN (
            SELECT new_id, company_id FROM fact_news_companies
        )
    """)
    print("Data inserted into fact_news_companies successfully!")
    # Close the DuckDB connection
    conn.close()
    spark.stop()


if __name__ == "__main__":
    process_news()
    print("Process completed successfully.")