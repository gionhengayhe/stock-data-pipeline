from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from scripts.elt_to_dwh.create_dwh import create_dwh
from scripts.elt_to_dwh.extract.crawl_news import crawl_news
from scripts.elt_to_dwh.extract.crawl_ohlcs import crawl_ohlcs
from scripts.elt_to_dwh.stage import (
    convert_news_to_parquet,
    convert_ohlcs_to_parquet,
    extract_companies_to_parquet,
    upload_daily_artifacts,
)
from scripts.quality.validate_dwh import validate_dwh


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

SPARK_CONF = {
    "spark.pyspark.python": "/usr/local/bin/python3.12",
    "spark.pyspark.driver.python": "/usr/local/bin/python3.12",
}
SPARK_ENV = {
    "PYSPARK_PYTHON": "/usr/local/bin/python3.12",
    "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.12",
}


def spark_job(
    task_id: str, trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/airflow/scripts/elt_to_dwh/transform/{task_id}.py",
        conn_id="spark_conn",
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        application_args=["{{ ds }}"],
        trigger_rule=trigger_rule,
    )

with DAG(
    dag_id='elt_to_dwh',
    start_date=datetime(2026, 8, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "elt", "warehouse"],
) as dag:
    create_dwh_task = PythonOperator(
        task_id='create_dwh',
        python_callable=create_dwh
    )
    with TaskGroup('extract_task') as extract_group:
        crawl_news_task = PythonOperator(
            task_id='crawl_news',
            python_callable=crawl_news,
            retries = 0
        )
        crawl_ohlcs_task = PythonOperator(
            task_id='crawl_ohlcs',
            python_callable=crawl_ohlcs
        )
        convert_news_to_parquet_task = PythonOperator(
            task_id='convert_news_to_parquet',
            python_callable=convert_news_to_parquet
        )
        convert_ohlcs_to_parquet_task = PythonOperator(
            task_id='convert_ohlcs_to_parquet',
            python_callable=convert_ohlcs_to_parquet
        )
        extract_companies_task = PythonOperator(
            task_id='extract_companies_to_parquet',
            python_callable=extract_companies_to_parquet
        )
        crawl_news_task >> convert_news_to_parquet_task
        crawl_ohlcs_task >> convert_ohlcs_to_parquet_task

    load_to_datalake_task = PythonOperator(
        task_id='load_to_datalake',
        python_callable=upload_daily_artifacts,
    )
    with TaskGroup('transform_task') as transform_group:
        process_companies_task = spark_job("process_companies")
        process_news_task = spark_job("process_news", TriggerRule.ALL_DONE)
        process_ohlcs_task = spark_job("process_ohlcs")

        # DuckDB is a local single-writer warehouse, so the Spark jobs commit
        # their distributed results one at a time.
        process_companies_task >> process_ohlcs_task >> process_news_task

    validate_dwh_task = PythonOperator(
        task_id='validate_dwh',
        python_callable=validate_dwh,
        op_kwargs={"expected_date": "{{ ds }}"},
    )

    create_dwh_task >> extract_group >> load_to_datalake_task >> transform_group
    [process_ohlcs_task, process_news_task] >> validate_dwh_task



