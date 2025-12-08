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
    import psycopg2 as pg
    import ast

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': context['ds'], # Строка в формате 2024-01-01
        # Лучше использовать логическую дату запуска дага 
        # Если @daily, то будет предыдущий день
        # Если @weekly, то предыдущая неделя и т.д
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

            cursor.execute("INSERT INTO mikhail_k_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()

def combine_data(**context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO mikhail_k_agg_table
        SELECT lti_user_id,
            attempt_type,
            COUNT(1) AS cnt_attempt,
            COUNT(attempt_type) FILTER (WHERE is_correct) AS cnt_correct,
            '{context['ds']}'::timestamp AS date
        FROM mikhail_k_table 
        WHERE created_at >= '{context['ds']}'::timestamp
        AND created_at < '{context['ds']}'::timestamp + INTERVAL '7 days'
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

def upload_agg_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM mikhail_k_agg_table
        WHERE date >= '{context['ds']}'::timestamp
        AND date < '{context['ds']}'::timestamp + INTERVAL '7 days';
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
        Key=f"mikhail_k_agg_{context['ds']}.csv"
    )

def upload_raw_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM mikhail_k_table
        WHERE date >= '{context['ds']}'::timestamp
        AND date < '{context['ds']}'::timestamp + INTERVAL '7 days';
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
        Key=f"mikhail_k_raw_{context['ds']}.csv"
    )

with DAG(
    dag_id="mikhail_k_lesson_8_9_practice", # Имя дага
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

    dag_start >> load_from_api >> upload_raw_data >> dag_end

    dag_start >> load_from_api >> combine_data >> upload_agg_data >> dag_end