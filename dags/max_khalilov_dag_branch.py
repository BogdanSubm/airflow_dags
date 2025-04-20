from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from max_khalilov_branch_operator import CustomBranchOperator
from api_operator_max_khalilov import ApiToPostgresOperator

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 4, 12),
}

with DAG(
    dag_id='max_khalilov_dag_branch',
    tags=['max_khalilov', '7'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    branch = CustomBranchOperator(
        task_id='branch', 
    )

    load_from_api = ApiToPostgresOperator(
        task_id='load_from_api',
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )

    dag_start >> branch >> load_from_api >> dag_end