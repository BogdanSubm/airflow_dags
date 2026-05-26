from datetime import datetime,timedelta 

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

def combine_data(**context):
    import psycopg2 as pg

    sql_querry = f""" 
        insert into marshavesent_agg_table
        select lti_user_id, attempt_type, count(1) as attempt_count,
        count(case when is_correct  then Null else 1  end) as attempt_failed_cnt,
        '{context['ds']}'::timestamp 
        from marshavesent_table
        where created_at >= '{context['ds']}'::timestamp 
            and created_at < '{context['ds']+timedelta(days=1)}'::timestamp 
        group by lti_user_id, attempt_type
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        sslmode='disable',
        keepalives_idle=600,
        tcp_user_timeout=600,
        connect_timeout=600
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_querry)
            conn.commit()

def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f""" 
        select lti_user_id, attempt_type, attempt_count, attempt_failed_cnt, date
        from marshavesent_agg_table
        where created_at >= '{context['ds']}'::timestamp 
            and created_at < '{context['ds']+timedelta(days=1)}'::timestamp 
    """

    conn = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        sslmode='disable',
        keepalives_idle=600,
        tcp_user_timeout=600,
        connect_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
    
    file = BytesIO()

    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerows(rows)
    file.seek(0)

    connection_s3= BaseHook.get_connection('conn_s3')

    s3_client = s3.client(
        's3',
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        endpoint_url=connection_s3.host,
        config=Config(signature_version='s3v4', connect_timeout=600, read_timeout=600)
    )

    s3_client.put_object(
        Body=file,
        Bucket='marshavesent-bucket',
        Key=f'marshavesent_{context["ds"]}.csv'
    )
    file.close()
    
with DAG(
    'lesson_9_repeat',
    tags=['marshavesent'],
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False
) as dag: 
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    combine_data = PythonOperator(task_id='combine_data', python_callable=combine_data)
    upload_data = PythonOperator(task_id='upload_data', python_callable=upload_data)

    dag_start >> combine_data >> upload_data >> dag_end
