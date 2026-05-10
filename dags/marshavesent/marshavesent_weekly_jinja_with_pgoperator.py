from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from marshavesent.operators_practise.postgres_execute_operator import PostgresExecuteOperator
from marshavesent.src_practise_1.utils_2 import (
    load_raw_data, export_raw_to_csv, export_aggregated_to_csv,
    CREATE_AGG_TABLE_SQL, AGGREGATION_SQL, CHECK_WEEKLY_DATA_SQL
)

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': 600,
    'depends_on_past': False,
}

# Макросы Jinja
def get_week_start(execution_date):
    dt = pendulum.instance(execution_date)
    return dt.start_of('week').to_date_string()

def get_week_end(execution_date):
    dt = pendulum.instance(execution_date)
    return dt.end_of('week').to_date_string()

with DAG(
    dag_id='marshavesent_weekly_jinja_operator_lesson_12',
    tags=['marshavesent', 'weekly', 'jinja', 'postgres_operator'],
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * 1',
    start_date=pendulum.datetime(2024, 1, 1),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    description='Еженедельная выгрузка с PostgresOperator и Jinja',
    render_template_as_native_obj=True,
    user_defined_macros={
        'week_start': get_week_start,
        'week_end': get_week_end
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    # 1. Загрузка сырых данных через API
    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        provide_context=True,
        op_kwargs={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    # 2. Создание таблицы агрегации (если не существует)
    create_agg_table = PostgresExecuteOperator(
        task_id='create_agg_table',
        sql=CREATE_AGG_TABLE_SQL,
        postgres_conn_id='conn_pg',
        database='etl'
    )
    
    # 3. Проверка загруженных данных
    check_raw_data = PostgresExecuteOperator(
        task_id='check_raw_data',
        sql=CHECK_WEEKLY_DATA_SQL,
        postgres_conn_id='conn_pg',
        database='etl',
        parameters={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        },
        show_result=True
    )
    
    # 4. Агрегация данных
    aggregate_data = PostgresExecuteOperator(
        task_id='aggregate_data',
        sql=AGGREGATION_SQL,
        postgres_conn_id='conn_pg',
        database='etl',
        parameters={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        },
        show_result=True
    )
    
    # 5. Экспорт сырых данных в CSV
    export_raw_csv = PythonOperator(
        task_id='export_raw_to_csv',
        python_callable=export_raw_to_csv,
        provide_context=True,
        op_kwargs={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    # 6. Экспорт агрегированных данных в CSV
    export_agg_csv = PythonOperator(
        task_id='export_aggregated_to_csv',
        python_callable=export_aggregated_to_csv,
        provide_context=True,
        op_kwargs={
            'week_start': '{{ week_start(execution_date) }}',
            'week_end': '{{ week_end(execution_date) }}'
        }
    )
    
    # Определение зависимостей
    dag_start >> load_raw_data_task >> create_agg_table >> check_raw_data >> aggregate_data
    aggregate_data >> [export_raw_csv, export_agg_csv] >> dag_end