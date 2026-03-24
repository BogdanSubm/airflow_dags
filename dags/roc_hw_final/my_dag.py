from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from roc_hw_final.operators import MySqlOperator
from roc_hw_final.config import config


DEFAULT_ARGS = {
    'owner': 'roc',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16)
}

with DAG(
    dag_id='roc_dynamic_tasks',
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['roc'],
    doc_md="Create dynamic tasks according the config"
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    for task_parameters in config:
        my_dynamic_task = MySqlOperator(
            task_id=f"task_{task_parameters['table_name']}",
            pg_conn_id='conn_pg',
            s3_conn_id='conn_s3',
            task_parameters=task_parameters
        )

        start >> my_dynamic_task >> end
