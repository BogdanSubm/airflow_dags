from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from marshavesent.custom_sensors.multi_external_task_sensor import MultiExternalTaskSensor

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='demo_multi_external_sensor',
    tags=['marshavesent', 'sensor', 'external'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Демонстрация MultiExternalTaskSensor',
    render_template_as_native_obj=True,
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # Сенсор ждет завершения задач в других DAG
    wait_for_external_tasks = MultiExternalTaskSensor(
        task_id='wait_for_external_tasks',
        external_tasks=[
            {
                'external_dag_id': 'test_external_dag',       
                'external_task_id': 'external_task_1',          
                'allowed_states': ['success']
            }
        ],
        mode='reschedule',
        poke_interval=300,
        timeout=7200
    )
    
    def process_after_external(**context):
        print("✅ Все внешние задачи завершены!")
        print("Начинаем финальную обработку и формирование отчетов...")
    
    final_task = PythonOperator(
        task_id='final_processing',
        python_callable=process_after_external
    )
    
    start >> wait_for_external_tasks >> final_task >> end