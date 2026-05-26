from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum

from marshavesent.src_practise_1.utils_2 import (load_raw_data, aggregate_data, 
    export_raw_to_csv, export_aggregated_to_csv)

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

# Пользовательский макрос Jinja для вычисления начала месяца
def get_week_start(execution_date):
    """Макрос для получения начала недели"""
    dt = pendulum.instance(execution_date)
    return dt.start_of('week').to_date_string()

def get_week_end(execution_date):
    """Макрос для получения конца недели"""
    dt = pendulum.instance(execution_date)
    return dt.end_of('week').to_date_string()

with DAG(
    dag_id='marshavesent_weekly_jinja',
    tags=['marshavesent', 'weekly', 'jinja'],
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * 1',  # Каждый понедельник
    start_date=pendulum.datetime(2024, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Еженедельная выгрузка с Jinja шаблонами',
    render_template_as_native_obj=True,
    user_defined_macros={
        'week_start': get_week_start,
        'week_end': get_week_end
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    # Используем Jinja шаблоны для передачи параметров
    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        provide_context=True,
        params={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True,
        params={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    export_raw_csv_task = PythonOperator(
        task_id='export_raw_to_csv',
        python_callable=export_raw_to_csv,
        provide_context=True,
        params={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    export_agg_csv_task = PythonOperator(
        task_id='export_aggregated_to_csv',
        python_callable=export_aggregated_to_csv,
        provide_context=True,
        params={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    dag_start >> load_raw_data_task >> aggregate_data_task
    aggregate_data_task >> [export_raw_csv_task, export_agg_csv_task] >> dag_end