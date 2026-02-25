from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'spiridonov_a',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}

def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM spiridonov_agg_table_les9
        WHERE date >= '{context["ds"]}'::timestamp
        AND date < '{context["ds"]}'::timestamp + INTERVAL '1 days';
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
            tcp_user_timeout=600,
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
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_pg')

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"spiridonov_les9_{context['ds']}.csv",
    )

def combine_data(**context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO spiridonov_agg_table_les9
        SELECT lti_user_id,
                attempt_type,
                COUNT(1),
                COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
                '{context["ds"]}'::timestamp
        FROM spiridonov_admin_table_8
        WHERE created_at >= '{context["ds"]}'::timestamp
        AND created_at < '{context["ds"]}'::timestamp + INTERVAL '1 days'
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
        tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()

with DAG(
    dag_id='spiridonov_lesson_9_dag',
    tags=['9', 'spiridonov'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
    )

    dag_start >> combine_data >> upload_data >> dag_end