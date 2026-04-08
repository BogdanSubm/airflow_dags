from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum

DEFAULT_ARGS = {
    'owner': 'iravoronina',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 3, 7),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def get_week_bounds(ds): # Принимает дату запуска Airflow и возвращает дату начала (понедельник) и конца (воскресенье) соответствующей недели.
    dt = pendulum.parse(ds)
    return dt.start_of('week').to_date_string(), dt.end_of('week').to_date_string()

def load_from_api(**context):
    import requests
    import psycopg2 as pg
    import ast

    start, end = get_week_bounds(context['ds'])

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': start,
        'end': end
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

         #ИДЕМПОТЕНТНОСТЬ: Удаляем данные за этот период перед вставкой
        cursor.execute("""
            DELETE FROM iravoronina_table 
            WHERE created_at >= %s::timestamp 
              AND created_at < %s::timestamp + INTERVAL '1 day'
        """, (start, end))

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

            cursor.execute("INSERT INTO iravoronina_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()

def combine_data(**context):
    import psycopg2 as pg

    start, end = get_week_bounds(context['ds'])

    sql_query = f"""
        INSERT INTO iravoronina_agg_table
        SELECT lti_user_id,
            attempt_type,
            COUNT(1) AS attempt_count,
            COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
            '{start}'::timestamp AS date
            FROM iravoronina_table 
            WHERE created_at >= '{start}'::timestamp
            AND created_at < '{end}'::timestamp + INTERVAL '1 day'
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
        # ИДЕМПОТЕНТНОСТЬ: Удаляем старую агрегацию за эту неделю
        cursor.execute("DELETE FROM iravoronina_agg_table WHERE date = %s::timestamp", (start,))
        cursor.execute(sql_query)
        conn.commit()

def upload_raw_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    
    start, end = get_week_bounds(context['ds'])

    # Выгружаем данные, которые попали в диапазон недели
    sql_query = f"""
        SELECT * FROM iravoronina_table
        WHERE created_at >= '{start}'::timestamp
          AND created_at < '{end}'::timestamp + INTERVAL '1 day';
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
        Key=f"iravoronina_hw9_raw_{start}.csv"
    )

def upload_agg_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    
    start, end = get_week_bounds(context['ds'])

    # Выгружаем данные, которые попали в диапазон недели
    sql_query = f"""
        SELECT * FROM iravoronina_agg_table
        WHERE date = '{start}'::timestamp;
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
        Key=f"iravoronina_hw9_agg_{start}.csv"
    )


with DAG(
    dag_id="iravoronina_hw_8_9",
    tags=['iravoronina'],
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

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )

    upload_agg_data = PythonOperator(
        task_id='upload_agg_data',
        python_callable=upload_agg_data,
    )

    upload_raw_data = PythonOperator(
        task_id='upload_raw_data',
        python_callable=upload_raw_data,
    )

    dag_start >> load_from_api >> combine_data >> upload_agg_data >> dag_end

    dag_start >> load_from_api >> upload_raw_data >> dag_end

    