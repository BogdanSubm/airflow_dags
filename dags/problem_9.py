from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'max_khalilov', # Автор DAG
    'retries': 2, # Количество повторов
    'retry_delay': 600, # Задержка между повторами
    'start_date': datetime(2025, 4, 6) # Дата начала выполнения
}

API_URL = 'https://b2b.itresume.ru/api/statistics'

def load_from_api(**context):
    import requests 
    import psycopg2 as pg
    import ast 

    execution_date = context['execution_date']
    start_of_week = execution_date - timedelta(days=execution_date.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(hours=6, minutes=59, seconds=59)

    start_str = start_of_week.strftime('%Y-%m-%d')
    end_str = end_of_week.strftime('%Y-%m-%d')

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': start_str,
        'end': end_str
    }
    response = requests.get(API_URL, params=payload)
    data = response.json()

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE maks_khalilov")
    
        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))

            cursor.execute("INSERT INTO maks_khalilov VALUES (%s, %s, %s, %s, %s, %s, %s)",row)

        conn.commit() # Сохранение изменений в базе данных

with DAG(
    dag_id='makskhalilowyandexru',
    tags=['7', 'homework', 'max'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1   
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end= EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api
    )

    dag_start >> load_from_api >> dag_end