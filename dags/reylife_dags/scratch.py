from datetime import datetime
import requests
import psycopg2
import json
import hashlib

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'reylife',
    'retries': 2,
    'start_date': datetime(2026, 6, 12)
}

API_URL = 'https://b2b.itresume.ru/api/statistics'


def get_data(**context):

    # забираем данные из апи
    r = requests.get(
        API_URL,
        params={
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': '2024-11-13',
            'end': '2024-11-14'
        }
    )
    data = r.json()
    print(f"получили записей: {len(data)}")

    # подключаемся к бд
    conn = BaseHook.get_connection('conn_pg')
    db = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cur = db.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reylife_raw (
            lti_user_id     TEXT,
            passback_params TEXT,
            is_correct      BOOLEAN,
            attempt_type    TEXT,
            created_at      TIMESTAMP,
            PRIMARY KEY (lti_user_id, created_at)
        )
    """)

    inserted = 0
    for row in data:
        cur.execute("""
            INSERT INTO reylife_raw (
                lti_user_id,
                passback_params,
                is_correct,
                attempt_type,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row.get('lti_user_id'),
            row.get('passback_params'),
            row.get('is_correct'),
            row.get('attempt_type'),
            row.get('created_at')
        ))

        if cur.rowcount:
            inserted += 1

    db.commit()
    db.close()

    print(f"вставили новых строк: {inserted}")


with DAG(
    dag_id='reylife_test2_dag',
    tags=['reylife', '@TvoiRaiii'],
    schedule='@once',
    default_args=DEFAULT_ARGS
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    load_data = PythonOperator(task_id='load_data', python_callable=get_data)
    dag_end = EmptyOperator(task_id='dag_end')

    dag_start >> load_data >> dag_end