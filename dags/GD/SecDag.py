from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from GD.src.utils import extract_data
import logging

def on_task_failure(context):
    """Callback при ошибке задачи."""
    logging.error(f"Задача {context['task_instance'].task_id} упала: {context['exception']}")

default_args = {
    'owner': 'GD',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Увеличили количество повторов
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_task_failure,  # Callback при ошибке
}

with DAG(
    dag_id='GD_Extract_Api',
    default_args=default_args,
    description='DAG для извлечения данных из API',
    schedule_interval='3 8 * * *',
    catchup=False,
    tags=['gd', 'api', 'extract'],
    max_active_runs=1,  # Ограничение одновременных запусков
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=extract_data,
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )

    dag_start >> load_from_api >> dag_end