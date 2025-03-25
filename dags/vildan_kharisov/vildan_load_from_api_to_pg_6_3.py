from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from vildan_kharisov.vildan_api_to_pg_operator import APIToPgOperator
from vildan_kharisov.vildan_branch_operator import CustomBranchOperator
from vildan_kharisov.vildan_pg_operator import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'vildan_kharisov',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 3, 17),
}



with DAG(
    dag_id="vildan_load_from_api_to_pg_with_operator_and_branch",
    tags=['6', 'vildan'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    branch = CustomBranchOperator(
        task_id='branch'
    )

    load_from_api = APIToPgOperator(
        task_id='load_from_api',
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )
    sql_to_pg = PostgresOperator(
        task_id='sql_to_pg',
        #sql_query = sql_query,
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )

    dag_start >> branch >> load_from_api >> sql_to_pg >> dag_end
