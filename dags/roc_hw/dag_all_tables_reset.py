from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from roc_hw.plugins.operators import TablesResetOperator


DEFAULT_ARGS = {
    'owner': 'roc',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2026, 2, 16)
}

with DAG(dag_id='roc_all_tables_reset',
         schedule=None,
         default_args=DEFAULT_ARGS,
         tags=['roc'],
         params={'delete/reset': 'delete'},
         ) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    reset = TablesResetOperator(
        task_id='reset',
        conn_id='conn_pg',
    )

    dag_start >> reset >> dag_end
