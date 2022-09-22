"""
Flow to get sales and convert to avro
"""

import os
import json
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


BASE_DIR = os.environ.get("BASE_DIR")
RAW_DIR = os.path.join(BASE_DIR, "raw", "sales", "{{ ds }}")
STG_DIR = os.path.join(BASE_DIR, "stg", "sales", "{{ ds }}")

dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval="0 1 * * *",
    catchup=True,
    tags=['de2022'],
    default_args=DEFAULT_ARGS,
)

extract_data_from_api = SimpleHttpOperator(
    task_id='extract_data_from_api',
    http_conn_id='api_scrape_sales',
    method='POST',
    data=json.dumps({"date": "{{ ds }}","raw_dir": RAW_DIR}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

convert_to_avro = SimpleHttpOperator(
    task_id='convert_to_avro',
    http_conn_id='api_convert_avro',
    method='POST',
    data=json.dumps({"raw_dir": RAW_DIR,"stg_dir": STG_DIR}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

extract_data_from_api >> convert_to_avro
