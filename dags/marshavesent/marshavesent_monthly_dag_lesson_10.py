from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum

from marshavesent.src_practise_1.utils_2 import load_raw_data_monthly, export_raw_monthly_to_csv

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

# Макросы Jinja для вычисления границ месяца
def get_month_start(execution_date):
    """Макрос для получения начала месяца"""
    dt = pendulum.instance(execution_date)
    return dt.start_of('month').to_date_string()

def get_month_end(execution_date):
    """Макрос для получения конца месяца"""
    dt = pendulum.instance(execution_date)
    return dt.end_of('month').to_date_string()

with DAG(
    dag_id='marshavesent_monthly_statistics',
    tags=['marshavesent', 'monthly', 'star'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',  # Запуск каждый день
    start_date=pendulum.datetime(2024, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Ежедневная выгрузка месячных данных с Jinja макросами',
    render_template_as_native_obj=True,
    user_defined_macros={
        'month_start': get_month_start,
        'month_end': get_month_end
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    # Загрузка сырых данных за месяц
    load_monthly_data_task = PythonOperator(
        task_id='load_monthly_data',
        python_callable=load_raw_data_monthly,
        provide_context=True,
        params={
            'month_start': '{{ month_start(execution_date) }}',
            'month_end': '{{ month_end(execution_date) }}'
        }
    )
    
    # Экспорт месячных данных в CSV
    export_monthly_csv_task = PythonOperator(
        task_id='export_monthly_csv',
        python_callable=export_raw_monthly_to_csv,
        provide_context=True,
        params={
            'month_start': '{{ month_start(execution_date) }}',
            'month_end': '{{ month_end(execution_date) }}'
        }
    )
    
    dag_start >> load_monthly_data_task >> export_monthly_csv_task >> dag_end