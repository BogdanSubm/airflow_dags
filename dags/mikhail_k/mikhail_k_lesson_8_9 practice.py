from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'mikhail_k',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def load_from_api(**context):
    import requests
    import pendulum

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': context['ds'],
        'end': pendulum.parse(context['ds']).add(days=7).to_date_string(),
    }
    response = requests.get(API_URL, params=payload)
    return response.json()

def write_data_to_db(data):
    import psycopg2 as pg
    import ast

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

            cursor.execute("INSERT INTO mikhail_k_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()

with DAG(
    dag_id="mikhail_k_lesson_8_practice",   # Имя дага
    tags=['mikhail_k'],                     # Теги для поиска
    schedule='@weekly',                     # Расписание запуска: ежедневно
    default_args=DEFAULT_ARGS,              # Аргументы из переменной выше
    max_active_runs=1,                      # Сколько одновременно дагранов будет работать
    max_active_tasks=1                      # В одном дагране может работать только одна таска
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
    )

    write_data_to_db = PythonOperator(
        task_id='write_data_to_db',
        python_callable=write_data_to_db,
    )    

    dag_start >> load_from_api >> write_data_to_db >> dag_end