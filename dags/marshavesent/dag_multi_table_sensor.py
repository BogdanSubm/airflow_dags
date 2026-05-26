from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from marshavesent.custom_sensors.multi_table_sql_sensor import MultiTableSQLSensor

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='demo_multi_table_sensor',
    tags=['marshavesent', 'sensor', 'sql'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    description='Демонстрация MultiTableSQLSensor',
    render_template_as_native_obj=True,
) as dag:
    
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
    
    # Симуляция обработки данных
    def process_data(**context):
        print("✅ Все таблицы готовы! Начинаем обработку данных...")
        print("Можно запускать агрегацию и экспорт")
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )
    
    start >> wait_for_tables >> process_task >> end