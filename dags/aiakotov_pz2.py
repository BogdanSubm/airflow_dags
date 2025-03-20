import ast
import codecs
import csv
from datetime import datetime
from io import BytesIO

import boto3 as s3
import pendulum
import psycopg2 as pg
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from botocore.client import Config

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}
RAW_TABLE = 'iakotov_pz1_raw_table'
AGG_TABLE = 'iakotov_pz1_agg_table'

API_URL = "https://b2b.itresume.ru/api/statistics"


def load_from_api(**context):
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': context['ds'],
        'end': pendulum.parse(context['ds']).add(days=1).to_date_string(),
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

            cursor.execute(f"INSERT INTO {RAW_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()


def aggregate_data(**context):
    sql_query = f"""
        INSERT INTO {AGG_TABLE}
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
               '{context['ds']}'::timestamp
          FROM {RAW_TABLE}
         WHERE created_at >= '{context['ds']}'::timestamp 
               AND created_at < '{context['ds']}'::timestamp + INTERVAL '1 days'
          GROUP BY lti_user_id, attempt_type;
    """

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
        cursor.execute(sql_query)
        conn.commit()


def upload_data(table_name, file_name, **context):
    sql_query = f"""
        SELECT * FROM {table_name}
        WHERE date >= '{context['ds']}'::timestamp 
              AND date < '{context['ds']}'::timestamp + INTERVAL '1 days';
    """

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
        cursor.execute(sql_query)
        data = cursor.fetchall()

    file = BytesIO()

    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4"),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"{file_name}.csv"
    )


def upload_raw(**context):
    upload_data(RAW_TABLE, f"RAW_{context['ds']}.csv")


def upload_agg(**context):
    upload_data(AGG_TABLE, f"AGG_{context['ds']}.csv")


with DAG(
        dag_id="aiakotov_pz2",
        tags=['4', 'admin'],
        schedule='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        max_active_tasks=1
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
    )
    aggregate_data = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
    )
    upload_raw = PythonOperator(
        task_id='upload_raw',
        python_callable=upload_raw,
    )
    upload_agg = PythonOperator(
        task_id='upload_agg',
        python_callable=upload_agg,
    )

    dag_start >> load_from_api >> aggregate_data >> upload_raw >> dag_end
    dag_start >> load_from_api >> aggregate_data >> upload_agg >> dag_end
