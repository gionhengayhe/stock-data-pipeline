import polars as pl
import duckdb
import boto3
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """
    Create and return a Spark session configured for S3 access.
    :return: SparkSession
    """
    spark = SparkSession.builder \
        .appName("process_ohlcs") \
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

def process_ohlcs():
    execution_date_str = sys.argv[1]
    print("Execution date string:", execution_date_str)
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    bucket = os.getenv("BUCKET_NAME")
    prefix = "ohlcs"
    parquet_path = get_parquet_path_by_date(bucket, prefix, execution_date.strftime("%Y%m%d"))
    print("Reading from:", parquet_path)

    spark = get_spark_session()

    df = spark.read.parquet(parquet_path)
    print("DataFrame loaded from Parquet:")
    df.printSchema()
    df.show(5)
    conn = duckdb.connect("/opt/airflow/database/config_dwh/mydb.duckdb")

    # Insert execution date into dim_time if not exists
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

    # Get the time_id for the execution date
    id_time_df = conn.execute(f'''
            SELECT id FROM dim_time WHERE date = '{execution_date}'
        ''').fetchdf()
    print("Time ID DataFrame:")
    print(id_time_df)

    id_company_df = spark.createDataFrame(conn.execute(f'''
            SELECT id, ticker FROM dim_companies
    ''').fetchdf())

    companies_df = id_company_df.dropDuplicates(['ticker'])
    print("Companies DataFrame:")
    print(companies_df)

    df = df.join(companies_df, on='ticker', how='left')
    df = df.filter(df['id'].isNotNull()).withColumnRenamed('id', 'company_id')
    df = df.filter(df['volume_weighted'].isNotNull())
    df = df.withColumn('time_id', lit(id_time_df['id'][0]))
    print("Transformed DataFrame:")
    print(df.show(5))
    arrow = pl.from_pandas(df.toPandas()).to_arrow()
    conn.register("temp_candles", arrow)
    # Load df into fact_candles table
    conn.execute("""
        INSERT INTO fact_candles (
            company_id, time_id, open, high, low, close, volume, volume_weighted, time_stamp, num_of_trades, is_otc
        )
        SELECT 
            company_id, time_id, open, high, low, close, volume, volume_weighted, time_stamp, num_of_trades, is_otc 
        FROM temp_candles
        WHERE NOT EXISTS (
            SELECT 1 
            FROM fact_candles 
            WHERE company_id = temp_candles.company_id and time_id = temp_candles.time_id
        )
        """)
    conn.close()
    print("Data has been successfully inserted into fact_candles in DuckDB!")
    spark.stop()

if __name__ == "__main__":
    process_ohlcs()
    print("Process completed successfully.")