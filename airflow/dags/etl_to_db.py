from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

from scripts.etl_to_db.create_db import create_db, create_tables, create_indexes
from scripts.etl_to_db.extract.crawl_companies import crawl_companies
from scripts.etl_to_db.extract.crawl_markets import crawl_markets
from scripts.etl_to_db.load.load_to_db import load_to_db
from scripts.etl_to_db.transform.transform_to_db import transform_to_db

with DAG(
    dag_id='etl_to_db',
    start_date=datetime(2025, 5, 1),
    schedule_interval='@monthly',
    catchup=True
) as dag:
    with TaskGroup(group_id='ddl_task') as ddl_group:
        create_database_task = PythonOperator(
            task_id='create_database',
            python_callable=create_db,
        )
        create_tables_task = PythonOperator(
            task_id='create_tables',
            python_callable=create_tables,
        )
        create_indexes_task = PythonOperator(
            task_id='create_indexes',
            python_callable=create_indexes,
        )
        create_database_task >> create_tables_task >> create_indexes_task

    with TaskGroup(group_id='extract_task') as extract_group:
        crawl_companies_task = PythonOperator(
            task_id='crawl_companies',
            python_callable=crawl_companies,
        )
        crawl_markets_task = PythonOperator(
            task_id='crawl_markets',
            python_callable=crawl_markets
        )
        [crawl_companies_task, crawl_markets_task]

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_to_db
    )
    load_to_db_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db
    )

ddl_group >> extract_group >> transform_task >> load_to_db_task
