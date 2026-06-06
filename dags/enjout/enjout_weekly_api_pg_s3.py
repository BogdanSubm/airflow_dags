from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from enjout.src.utils import (
    aggregate_data,
    export_aggregated_to_s3,
    get_week_boundaries,
    load_raw_data,
)

DEFAULT_ARGS = {
    'owner': 'enjout',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='enjout_weekly_api_pg_s3',
    tags=['enjout', 'weekly', 'api', 'postgres', 's3'],
    description='Еженедельная выгрузка из API в PostgreSQL, агрегация и экспорт в S3',
    schedule='0 7 * * 1',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    get_week_boundaries_task = PythonOperator(
        task_id='get_week_boundaries',
        python_callable=get_week_boundaries,
    )

    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        execution_timeout=timedelta(hours=1),
    )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        execution_timeout=timedelta(hours=1),
    )

    export_aggregated_to_s3_task = PythonOperator(
        task_id='export_aggregated_to_s3',
        python_callable=export_aggregated_to_s3,
        execution_timeout=timedelta(hours=1),
    )

    (
        dag_start
        >> get_week_boundaries_task
        >> load_raw_data_task
        >> aggregate_data_task
        >> export_aggregated_to_s3_task
        >> dag_end
    )
