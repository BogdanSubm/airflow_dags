from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os
import json

# Добавляем пути
sys.path.insert(0, os.path.dirname(__file__))

from marshavesent.src_practise_1.utils_2 import load_raw_data_monthly, export_raw_monthly_to_csv
from operators_practise.postgres_execute_operator import PostgresBranchOperator

# Загружаем конфигурацию из файла
def load_config():
    """Загружает конфигурацию дней выполнения"""
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'monthly_run_days.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get('allowed_days', [1, 2, 5])
    return [1, 2, 5]  # Значения по умолчанию

# Загружаем разрешенные дни
ALLOWED_DAYS = load_config()

# Можно также использовать Python конфиг
try:
    from config import MONTHLY_RUN_DAYS
    ALLOWED_DAYS = MONTHLY_RUN_DAYS
except ImportError:
    pass

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

def get_month_start(execution_date):
    """Макрос для получения начала месяца"""
    dt = pendulum.instance(execution_date)
    return dt.start_of('month').to_date_string()

def get_month_end(execution_date):
    """Макрос для получения конца месяца"""
    dt = pendulum.instance(execution_date)
    return dt.end_of('month').to_date_string()

with DAG(
    dag_id='marshavesent_monthly_star',
    tags=['marshavesent', 'monthly', 'star', 'branch_operator'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',  # Запуск каждый день
    start_date=pendulum.datetime(2024, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Ежедневная проверка и месячная выгрузка с BranchOperator',
    render_template_as_native_obj=True,
    user_defined_macros={
        'month_start': get_month_start,
        'month_end': get_month_end
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    dag_skip = EmptyOperator(task_id='dag_skip')  # Задача для пропуска
    
    # BranchOperator для проверки дня месяца
    check_day = PostgresBranchOperator(
        task_id='check_day',
        allowed_days=ALLOWED_DAYS,  # Берем из конфига
        task_id_to_continue='load_monthly_data',
        task_id_to_skip='dag_skip'
    )
    
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
    
    # Определяем зависимости с ветвлением
    dag_start >> check_day
    
    # Ветка выполнения в разрешенные дни
    check_day >> load_monthly_data_task >> export_monthly_csv_task >> dag_end
    
    # Ветка пропуска в остальные дни
    check_day >> dag_skip >> dag_end