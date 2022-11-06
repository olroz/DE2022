"""
Sales processing pipeline
"""
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from table_defs.sales_bronze_csv import sales_bronze_csv


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="1-sales_flow",
    description="Ingest and process sales data",
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 9, 1),
    end_date=dt.datetime(2022, 10, 1),
    catchup=True,
    tags=['de2022'],
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(seconds=600),
)

dag.doc_md = __doc__


transfer_from_datalake_to_bronze = BigQueryInsertJobOperator(
    task_id='transfer_from_datalake_to_bronze',
    dag=dag,
    execution_timeout = timedelta(seconds=120),
    location='europe-west1',
    configuration={
        "query": {
            "query": "{% include 'sql/sales_from_datalake_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "sales_csv": sales_bronze_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de2022-raw",
        'project_id': "de2022-oleh-rozit"
    }
)

transfer_from_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='transfer_from_dwh_bronze_to_dwh_silver',
    dag=dag,
    execution_timeout = timedelta(seconds=120),
    location='europe-west1',
    project_id='de2022-oleh-rozit',
    configuration={
        "query": {
            "query": "{% include 'sql/sales_from_dwh_bronze_to_dwh_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de2022-oleh-rozit"
    }
)

transfer_from_datalake_to_bronze >> transfer_from_bronze_to_silver
