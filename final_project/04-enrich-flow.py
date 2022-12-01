"""
enrich_user_profiles_to_dwh_gold pipeline
"""

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="4-enrich_flow",
    description="enrich user_profiles to dwh gold",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['de2022'],
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(seconds=600),
)

dag.doc_md = __doc__


transfer_from_datalake_to_silver = BigQueryInsertJobOperator(
    task_id='enrich_user_profiles_to_dwh_gold',
    dag=dag,
    execution_timeout = timedelta(seconds=120),
    location='europe-west1',
    project_id='de2022-oleh-rozit',
    configuration={
        "query": {
            "query": "{% include 'sql/user_profiles_enrich_to_dwh_gold.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de2022-oleh-rozit"
    }
)
