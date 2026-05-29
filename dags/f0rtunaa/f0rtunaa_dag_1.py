from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'f0rtunaa',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def get_week_dates(ds):
    import pendulum
    last_week = pendulum.parse(ds).subtract(weeks=1)
    return last_week.start_of('week').to_date_string(), last_week.end_of('week').to_date_string()

def load_from_api(**context):
    import requests
    import psycopg2 as pg
    import ast

    start, end = get_week_dates(context['ds'])
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': start,
        'end': end,
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
        # Идемпотентность
        cursor.execute("""DELETE FROM f0rtunaa_raw_table 
                       WHERE created_at >= %s::timestamp 
               AND created_at < %s::timestamp""", (start,end,))
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

            cursor.execute("""INSERT INTO f0rtunaa_raw_table (
                           lti_user_id, 
                           is_correct, 
                           attempt_type,
                           created_at,
                           oauth_consumer_key,
                           lis_result_sourcedid,
                           lis_outcome_service_url
                           )
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""", row)

        conn.commit()

def agg_data(**context):
    import psycopg2 as pg

    start, end = get_week_dates(context['ds'])

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

        cursor.execute("""
            DELETE FROM f0rtunaa_agg_table 
            WHERE week_start = %s
        """, (start,))

        cursor.execute("""
            INSERT INTO f0rtunaa_agg_table (
                week_start,
                lti_user_id,
                total_attempts,
                correct_attempts,
                accuracy
            )
            SELECT
                %s AS week_start,
                lti_user_id,
                COUNT(*) AS total_attempts,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct_attempts,
                ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0 END) * 100, 2) AS accuracy
            FROM f0rtunaa_raw_table
            WHERE created_at >= %s::timestamp 
            AND created_at < %s::timestamp
            GROUP BY lti_user_id
        """, (start, start, end))


        conn.commit()

def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    start, end = get_week_dates(context['ds'])

    sql_query = """
        SELECT * FROM f0rtunaa_agg_table
        WHERE week_start = %s
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
        cursor.execute(sql_query, (start,))
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
        Key=f"f0rtunaa_{start}.csv"
    )



with DAG(
        dag_id="load_from_api_f0rtunaa",
        tags=['4', 'f0rtunaa'],
        schedule='@weekly',
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

    agg_data = PythonOperator(
        task_id='agg_data',
        python_callable=agg_data,
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data
    )

    dag_start >> load_from_api >> agg_data >> upload_data>> dag_end
