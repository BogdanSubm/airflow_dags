from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from marshavesent.config import MONTHLY_RUN_DAYS
from marshavesent.operators_practise.postgres_execute_operator import (
    PostgresExecuteOperator,
    PostgresBranchOperator
)
from marshavesent.src_practise_1.utils_2 import (
    load_raw_data_monthly,
    export_raw_monthly_to_csv,
    CREATE_MONTHLY_TABLE_SQL,
    CHECK_MONTHLY_DATA_SQL
)

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

# Макросы Jinja
def get_month_start(execution_date):
    dt = pendulum.instance(execution_date)
    return dt.start_of('month').to_date_string()

def get_month_end(execution_date):
    dt = pendulum.instance(execution_date)
    return dt.end_of('month').to_date_string()

with DAG(
    dag_id='marshavesent_monthly_star',
    tags=['marshavesent', 'monthly', 'star', 'branch'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Месячная загрузка с BranchOperator (1, 2, 5 числа)',
    render_template_as_native_obj=True,
    user_defined_macros={
        'month_start': get_month_start,
        'month_end': get_month_end
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    dag_skip = EmptyOperator(task_id='workflow_skipped')
    
    # BranchOperator: проверка дня месяца
    check_day = PostgresBranchOperator(
        task_id='check_execution_day',
        allowed_days=MONTHLY_RUN_DAYS,
        task_id_to_continue='create_monthly_table',
        task_id_to_skip='workflow_skipped'
    )
    
    # Создание таблицы
    create_table = PostgresExecuteOperator(
        task_id='create_monthly_table',
        sql=CREATE_MONTHLY_TABLE_SQL,
        postgres_conn_id='conn_pg',
        database='etl'
    )
    
    # Загрузка данных из API
    load_data = PythonOperator(
        task_id='load_monthly_data',
        python_callable=load_raw_data_monthly,
        provide_context=True,
        op_kwargs={
            'month_start': '{{ month_start(execution_date) }}',
            'month_end': '{{ month_end(execution_date) }}'
        }
    )
    
    # Проверка загруженных данных
    check_data = PostgresExecuteOperator(
        task_id='check_loaded_data',
        sql=CHECK_MONTHLY_DATA_SQL,
        postgres_conn_id='conn_pg',
        database='etl',
        parameters={
            'month_start': '{{ month_start(execution_date) }}',
            'month_end': '{{ month_end(execution_date) }}'
        },
        show_result=True
    )
    
    # Экспорт в CSV
    export_csv = PythonOperator(
        task_id='export_monthly_csv',
        python_callable=export_raw_monthly_to_csv,
        provide_context=True,
        op_kwargs={
            'month_start': '{{ month_start(execution_date) }}',
            'month_end': '{{ month_end(execution_date) }}'
        }
    )
    
    # ПРАВИЛЬНЫЕ ЗАВИСИМОСТИ ДЛЯ BRANCH
    dag_start >> check_day
    
    # Ветка 1: День разрешен - полный пайплайн
    check_day >> create_table >> load_data >> check_data >> export_csv >> dag_end
    
    # Ветка 2: День запрещен - только скип
    check_day >> dag_skip >> dag_end