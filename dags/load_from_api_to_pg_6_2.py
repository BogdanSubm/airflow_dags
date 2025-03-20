from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from operators.api_to_pg_operator import APIToPgOperator
from sensors.api_sensor import APISensor

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}


with DAG(
    dag_id="load_from_api_to_pg_with_operator",
    tags=['6', 'admin'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    sensor = APISensor(
        task_id='api_sensor',
        mode='reschedule',
        poke_interval=300,
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )

    load_from_api = APIToPgOperator(
        task_id='load_from_api',
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )

    dag_start >> sensor >> load_from_api >> dag_end

