from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor 
from airflow.sensors.time_delta import TimeDeltaSensor 
from airflow.hooks.base import BaseHook

from operator_combine_date_max_khalilov import CustomCombineDataOperator
from upload_data_operator_max_khalilov import CustomUploadDataOperator

DEFAULT_ARGS = {
    'owner': 'admin',
    'retires': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 4, 14)
}

with DAG(
    dag_id='max_khalilov_lesson14',
    tags=['max_khalilov', '7'],
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag')

    combine_data = CustomCombineDataOperator(
        task_id='combine_data',
    )

    upload_data = CustomUploadDataOperator(
        task_id='upload_data',
    )

    start_dag >> combine_data >> upload_data >> end_dag 



