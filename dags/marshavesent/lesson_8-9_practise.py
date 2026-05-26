from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum

from marshavesent.src_practise_1.utils_1 import (get_week_boundaries, load_raw_data, 
    aggregate_data, export_raw_to_csv, export_aggregated_to_csv)

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

API_URL = "https://b2b.itresume.ru/api/statistics"
BUCKET_NAME = 'marshavesent-bucket'

# DAG definition
with DAG(
    dag_id='marshavesent_weekly_statistics',
    tags=['marshavesent', 'weekly'],
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * 1',  #  Запуск каждый понедельник в 00:00
    start_date=pendulum.datetime(2024, 1, 1),  # Начальная дата Запуск с 01 января 2024 года
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Еженедельная выгрузка raw и agg данных'
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    # Вычисление границ недели
    get_week_boundaries = PythonOperator(
        task_id='get_week_boundaries',
        python_callable=get_week_boundaries,
        provide_context=True
    )
    
    # Загрузка сырых данных из API
    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        provide_context=True
    )
    
    # Агрегация данных в PostgreSQL
    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True
    )
    
    # Экспорт сырых данных в CSV (параллельная задача)
    export_raw_csv_task = PythonOperator(
        task_id='export_raw_to_csv',
        python_callable=export_raw_to_csv,
        provide_context=True
    )
    
    # Экспорт агрегированных данных в CSV (параллельная задача)
    export_agg_csv_task = PythonOperator(
        task_id='export_aggregated_to_csv',
        python_callable=export_aggregated_to_csv,
        provide_context=True
    )
    
    # Определение зависимостей задач с параллельным выполнением
    dag_start >> get_week_boundaries >> load_raw_data_task
    load_raw_data_task >> aggregate_data_task
    aggregate_data_task >> [export_raw_csv_task, export_agg_csv_task] >> dag_end