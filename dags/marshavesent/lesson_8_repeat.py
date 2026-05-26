from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'marshavesent',
    'start_date': datetime(2026, 2, 1),
    'retries': 1,
    'retry_delay': 600,
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def load_from_api(**context):
    import requests
    import pendulum
    import psycopg2 as pg
    import ast
    
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': context['ds'], 
        'end': pendulum.parse(context['ds']).add(days=1).to_date_string()
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
        connection_timeout=600,
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
            row.append(el.get('created'))
            row.append(passback_params.get('lesson_id'))
            row.append(passback_params.get('lesson_name'))
            row.append(el.get('score'))

            cursor.execute("INSERT INTO marshavesent_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)
        
        conn.commit()
    
with DAG(
    dag_id='marshavesent_lesson_8',
    tags=['marshavesent'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api_task = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api
    )

    dag_start >> load_from_api_task >> dag_end
