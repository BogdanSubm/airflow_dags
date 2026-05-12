from datetime import datetime,
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from marshavesent.custom_sensors.multi_table_sql_sensor import MultiTableSQLSensor
from marshavesent.custom_sensors.multi_external_task_sensor import MultiExternalTaskSensor

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 300,
}

# ============================================
# DAG 1: Демонстрация MultiTableSQLSensor
# ============================================
with DAG(
    dag_id='demo_multi_table_sensor',
    tags=['marshavesent', 'sensor', 'sql'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Демонстрация MultiTableSQLSensor',
    render_template_as_native_obj=True,
) as dag1:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # Сенсор ждет данные в нескольких таблицах
    wait_for_tables = MultiTableSQLSensor(
        task_id='wait_for_tables',
        tables=[
            'marshavesent_raw_data',
            'marshavesent_agg_data',
            'marshavesent_raw_data_monthly'
        ],
        postgres_conn_id='conn_pg',
        database='etl',
        min_rows=1,
        check_all=True,  # Все таблицы должны иметь данные
        mode='reschedule',
        poke_interval=300,
        timeout=3600
    )
    
    # Симуляция загрузки данных
    def check_data_availability(**context):
        print("Все таблицы готовы! Начинаем обработку...")
    
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=check_data_availability
    )
    
    start >> wait_for_tables >> process_data >> end


# ============================================
# DAG 2: Демонстрация MultiExternalTaskSensor
# ============================================
with DAG(
    dag_id='demo_multi_external_sensor',
    tags=['marshavesent', 'sensor', 'external'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Демонстрация MultiExternalTaskSensor',
    render_template_as_native_obj=True,
) as dag2:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # Сенсор ждет завершения задач в других DAG
    wait_for_external_tasks = MultiExternalTaskSensor(
        task_id='wait_for_external_tasks',
        external_tasks=[
            {
                'external_dag_id': 'marshavesent_weekly_jinja_lesson_10',
                'external_task_id': 'aggregate_data',
                'allowed_states': ['success']
            },
            {
                'external_dag_id': 'marshavesent_monthly_star',
                'external_task_id': 'load_monthly_data',
                'allowed_states': ['success']
            }
        ],
        mode='reschedule',
        poke_interval=300,
        timeout=7200
    )
    
    def process_after_external(**context):
        print("Все внешние задачи завершены! Начинаем финальную обработку...")
    
    final_task = PythonOperator(
        task_id='final_task',
        python_callable=process_after_external
    )
    
    start >> wait_for_external_tasks >> final_task >> end


# ============================================
# DAG 3: Тестовый DAG для проверки ExternalSensor
# ============================================
with DAG(
    dag_id='test_external_dag',
    tags=['marshavesent', 'test', 'external'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Тестовый DAG для ExternalTaskSensor',
) as dag3:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # Задача, которая имитирует долгую работу
    def slow_task(**context):
        sleep_time = random.randint(10, 30)
        print(f"Выполняю задачу {sleep_time} секунд...")
        time.sleep(sleep_time)
        print("Задача завершена!")
    
    task1 = PythonOperator(
        task_id='external_task_1',
        python_callable=slow_task
    )
    
    start >> task1 >> end