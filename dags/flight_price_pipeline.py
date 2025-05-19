# dags/flight_price_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Import task modules
from tasks.data_ingestion import csv_to_mysql
from tasks.utils.logging_config import configure_logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

# Define the DAG
dag = DAG(
    'flight_price_analysis',
    default_args=default_args,
    description='Flight Price Analysis Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['flight_price']
)

# Configure logging
logger = configure_logging('flight_price_pipeline')

# Define tasks
ingest_data = PythonOperator(
    task_id='ingest_data',
    python_callable=csv_to_mysql,
    op_kwargs={
        'csv_path': '/opt/airflow/dags/data/Flight_Price_Dataset_of_Bangladesh.csv',
        'mysql_conn_id': 'mysql_conn',
        'table_name': 'flight_data'
    },
    dag=dag
)

# Task dependencies
ingest_data
