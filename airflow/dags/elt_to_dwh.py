from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from scripts.elt_to_dwh.create_dwh import create_dwh
from scripts.elt_to_dwh.extract.crawl_news import crawl_news
from scripts.elt_to_dwh.extract.crawl_ohlcs import crawl_ohlcs
from scripts.elt_to_dwh.load.load_api_to_parquet import convert_news_to_parquet, convert_ohlcs_to_parquet
from scripts.elt_to_dwh.load.load_db_to_parquet import load_db_to_parquet
from scripts.elt_to_dwh.load.load_parquet_to_datalake import upload_to_s3
from scripts.elt_to_dwh.transform.process_companies import process_companies

with DAG(
    dag_id='elt_to_dwh',
    start_date=datetime(2025, 7, 10),
    end_date=datetime(2025,7,20),
    schedule_interval='@daily',
) as dag:
    create_dwh_task = PythonOperator(
        task_id='create_dwh',
        python_callable=create_dwh
    )
    with TaskGroup('extract_task') as extract_group:
        crawl_news_task = PythonOperator(
            task_id='crawl_news',
            python_callable=crawl_news
        )
        crawl_ohlcs_task = PythonOperator(
            task_id='crawl_ohlcs',
            python_callable=crawl_ohlcs
        )
        convert_news_to_parquet_task = PythonOperator(
            task_id='convert_news_to_parquet',
            python_callable=convert_news_to_parquet
        )
        convert_ohlcs_to_parquet = PythonOperator(
            task_id='convert_ohlcs_to_parquet',
            python_callable=convert_ohlcs_to_parquet
        )
        load_db_to_parquet_task = PythonOperator(
            task_id='load_db_to_parquet',
            python_callable=load_db_to_parquet
        )
        crawl_news_task >> convert_news_to_parquet_task
        crawl_ohlcs_task >> convert_ohlcs_to_parquet
        [convert_news_to_parquet_task, convert_ohlcs_to_parquet, load_db_to_parquet_task]

    load_to_datalake_task = PythonOperator(
        task_id='load_to_datalake',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_folder': '/opt/airflow/data/parquet'
        }
    )
    with TaskGroup('transform_task') as transform_group:
        process_companies_task = PythonOperator(
            task_id='process_companies',
            python_callable=process_companies
        )
        process_news_task = SparkSubmitOperator(
            task_id='process_news',
            application='/opt/airflow/scripts/elt_to_dwh/transform/process_news.py',
            conn_id='spark_conn',
            application_args=["{{ ds }}"],
        )

        process_ohlcs_task = SparkSubmitOperator(
            task_id='process_ohlcs',
            application='/opt/airflow/scripts/elt_to_dwh/transform/process_ohlcs.py',
            conn_id='spark_conn',
            application_args=["{{ ds }}"],
        )

        process_companies_task >> process_ohlcs_task >> process_news_task

    create_dwh_task >> extract_group >> load_to_datalake_task >> transform_group



