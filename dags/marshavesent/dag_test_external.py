from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import time
import random

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='test_external_dag',
    tags=['marshavesent', 'test', 'external'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Тестовый DAG для ExternalTaskSensor',
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # Задача, которая имитирует работу (от 10 до 30 секунд)
    def slow_task(**context):
        sleep_time = random.randint(10, 30)
        print(f"⏳ Выполняю задачу {sleep_time} секунд...")
        time.sleep(sleep_time)
        print("✅ Задача успешно завершена!")
    
    task1 = PythonOperator(
        task_id='external_task_1',
        python_callable=slow_task
    )
    
    start >> task1 >> end