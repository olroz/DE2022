"""
Profiles processing pipeline
"""

from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="3-profiles_flow",
    description="Ingest and process profiles data",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['de2022'],
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(seconds=600),
)

dag.doc_md = __doc__

transfer_from_datalake_to_silver = GCSToBigQueryOperator(
    task_id='transfer_from_dataklake_to_dwh_silver',
    dag=dag,
    execution_timeout = timedelta(seconds=210),
    location='europe-west1',
    bucket='de2022-raw',
    source_objects=['user_profiles/*.jsonl'],
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    destination_project_dataset_table='de2022-oleh-rozit.silver.profiles',
)
