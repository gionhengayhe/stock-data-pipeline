from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup

from scripts.etl_to_db.create_db import initialize_metadata_db
from scripts.etl_to_db.extract.crawl_companies import crawl_companies
from scripts.etl_to_db.extract.crawl_markets import crawl_markets
from scripts.etl_to_db.load.load_to_db import load_to_db
from scripts.etl_to_db.transform.transform_to_db import transform_to_db


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id='etl_to_db',
    start_date=datetime(2025, 5, 1),
    schedule='@monthly',
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "etl", "metadata"],
) as dag:
    initialize_task = PythonOperator(
        task_id="initialize_metadata_db",
        python_callable=initialize_metadata_db,
    )

    with TaskGroup(group_id='extract_task') as extract_group:
        crawl_companies_task = PythonOperator(
            task_id='crawl_companies',
            python_callable=crawl_companies,
        )
        crawl_markets_task = PythonOperator(
            task_id='crawl_markets',
            python_callable=crawl_markets
        )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_to_db
    )
    load_to_db_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db
    )

initialize_task >> extract_group >> transform_task >> load_to_db_task
