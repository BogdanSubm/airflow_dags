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
    # Получаем начало недели (понедельник)
    start_of_week = execution_date - timedelta(days=execution_date.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    # Получаем конец недели (воскресенье)
    end_of_week = start_of_week + timedelta(days=6)
    end_of_week = end_of_week.replace(hour=23, minute=59, second=59)

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

        # Проверка количества записей до очистки
        cursor.execute("SELECT COUNT(*) FROM maks_khalilov")
        count_before = cursor.fetchone()[0]

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

            cursor.execute("INSERT INTO maks_khalilov VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()

def agr_func(**context):
    import psycopg2 as pg

    sql_query = """
        INSERT INTO maks_khalilov_agr
        SELECT 
            lti_user_id,
            attempt_type,
            COUNT(CASE WHEN is_correct THEN 1 END) AS cnt_correct,
            COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) as attempt_fails_count,
            COUNT(*) AS cnt_attempts
        FROM maks_khalilov
        GROUP BY 1, 2;
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
        cursor.execute("TRUNCATE TABLE maks_khalilov_agr")
        cursor.execute(sql_query)
        conn.commit()
            

def upload_to_s3(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM maks_khalilov_agr
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
        config=Config(signature_version='s3v4')
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"max_khalilov_{context['ds']}.csv"
    )

def upload_raw_to_s3(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = """
        SELECT * FROM maks_khalilov
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
        config=Config(signature_version='s3v4')
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"max_khalilov_raw_{context['ds']}.csv"
    )

            
with DAG(
    dag_id='makskhalilowyandexru',
    tags=['7', 'homework', 'max'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1   
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api
    )

    agr_func = PythonOperator(
        task_id='agr_func',
        python_callable=agr_func
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_to_s3
    )

    upload_raw_data = PythonOperator(
        task_id='upload_raw_data',
        python_callable=upload_raw_to_s3
    )

    dag_start >> load_from_api >> agr_func >> upload_data >> dag_end
    dag_start >> load_from_api >> upload_raw_data >> dag_end
