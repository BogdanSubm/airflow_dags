from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor 
from airflow.hooks.base import BaseHook

from operators.operator_combine_date_max_khalilov import CustomCombineDataOperator
from operators.upload_data_operator_max_khalilov import CustomUploadDataOperator
from sensors.External_sens_max_kahlilov import MultiTableSqlSensor

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
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
    end_dagg = EmptyOperator(task_id='end_dag')

    wait_3_msk = TimeDeltaSensor(
        task_id='wait_3_msk', 
        delta=timedelta(hours=3), # параметр, сколько времени ждать. Обязательное поле
        mode='reschedule', # параметр, как будет работать датчик. Обязательное поле
        poke_interval=300, # параметр, как часто будет проверяться датчик. Обязательное поле
    )

    # Используем только MultiTableSqlSensor для проверки нескольких таблиц
    multi_table_sensor = MultiTableSqlSensor(
        task_id='max_khalilov_multi_table_sensor',
        tables=['maks_halilov', 'max_api_table', 'maks_khalilov_agr'],  # список таблиц для проверки
        date_filter=True,  # использовать фильтр по дате
        mode='reschedule',  # как будет работать датчик
        poke_interval=300,  # как часто будет проверяться датчик
    )

    combine_data = CustomCombineDataOperator(
        task_id='combine_data',
    )

    upload_data = CustomUploadDataOperator(
        task_id='upload_data',
    )

    # Обновляем последовательность задач, удаляя dag_sensor
    start_dag >> wait_3_msk >> multi_table_sensor >> combine_data >> upload_data >> end_dagg