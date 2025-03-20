from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 9, 1),
}

with DAG(
    dag_id="utils",
    tags=['utils', 'admin'],
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    pip_freeze = BashOperator(
        task_id='pip_freeze',
        bash_command='pip freeze'
    )

    dag_start >> pip_freeze >> dag_end
