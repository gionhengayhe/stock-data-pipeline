import polars as pl
import duckdb
import boto3
import fsspec
import os
import s3fs


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

    return f"s3://{bucket}/{files[0]}"

def process_companies(**kwargs):
    execution_date = kwargs['execution_date'].strftime("%Y%m%d")
    bucket = os.getenv("BUCKET_NAME")
    prefix = "companies"
    parquet_path = get_parquet_path_by_date(bucket, prefix,execution_date)
    print("Reading from:", parquet_path)

    fs = fsspec.filesystem(
        "s3",
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"region_name": os.getenv("AWS_REGION")}
    )
    with fs.open(parquet_path, 'rb') as f:
        df = pl.read_parquet(f)
    print("DataFrame loaded from Parquet:")
    print(df.schema)
    print(df.head(5))
    conn = duckdb.connect("/opt/airflow/database/config_dwh/mydb.duckdb")
    conn.register("temp_companies", df.to_arrow())
    # print(conn.execute("SHOW TABLES").fetchall())

    conn.execute("""
        INSERT INTO dim_companies (
            name, ticker, is_delisted, category,
            currency, location, exchange, region,
            industry, sector, sic_industry, sic_sector, updated_time
        )
        SELECT 
            name, ticker, is_delisted, category,
            currency, location, exchange, region,
            industry, sector, sic_industry, sic_sector, updated_time
        FROM temp_companies
        WHERE ticker NOT IN (
            SELECT ticker FROM dim_companies
        )   
    """)
    conn.close()
    print("Data has been successfully inserted into dim_companies in DuckDB!")

if __name__ == "__main__":
    process_companies()
