from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'reylife',
    'retries': 2,
    'start_date': datetime(2026, 6, 8)
}

API_URL = 'https://b2b.itresume.ru/api/statistics'
def get_data(**context):
    import  requests
    import psycopg2 as pg
    import ast
    import pendulum
    r = requests.get('https://b2b.itresume.ru/api/statistics',
                     params={'client': 'Skillfactory', 'client_key': 'M2MGWS', 'start': '2024-11-13',
                             'end': '2024-11-14'})
    data = r.json()
    print(data[1])
    # with pg.connect(
    #     dbname='mydb',
    #     sslmode='disable',
    #     user='',
    #     password=''
    #
    # )

with DAG(
    dag_id='reylife_test_dag',
    tags=['reylife', '@TvoiRaiii'],
    schedule='@once',
    default_args=DEFAULT_ARGS
) as dag:
    dag_start = EmptyOperator(task_id='dag_start', owner='reylife')
    load_data = PythonOperator(task_id='load_data', python_callable=get_data)
    dag_end = EmptyOperator(task_id='dag_end', owner='reylife')
    dag_start >> load_data >> dag_end
