"""
Flow to load sales csv to gcp
"""

import os
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


BASE_DIR = os.environ.get("BASE_DIR_CSV")
SRC_DIR = os.path.join(BASE_DIR, "{{ ds }}")
BUCKET_NAME = 'de2022_sales'
PREFIX = 'src1/sales/v1'


dag = DAG(
    dag_id='upload_sales',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval="0 1 * * *",
    catchup=True,
    tags=['de2022'],
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(seconds=600),
)

clean_up = GoogleCloudStorageDeleteOperator(
    task_id="clean_up",
    bucket_name=BUCKET_NAME,
    prefix= os.path.join(
            PREFIX,
            'year={{ execution_date.strftime("%Y") }}',
            'month={{ execution_date.strftime("%m") }}',
            'day={{ execution_date.strftime("%d") }}'
    ),
    execution_timeout = timedelta(seconds=120),
    dag=dag,
)

upload_sales = LocalFilesystemToGCSOperator(
    task_id="upload_sales",
    src = SRC_DIR+'/*.csv',
    dst= os.path.join(
        PREFIX,
        'year={{ execution_date.strftime("%Y") }}',
        'month={{ execution_date.strftime("%m") }}',
        'day={{ execution_date.strftime("%d") }}'
    ) +'/',
    bucket = BUCKET_NAME,
    execution_timeout = timedelta(seconds=300),
    dag=dag,
)


clean_up >> upload_sales
