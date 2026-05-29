from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 2, 16),
}

def combine_data(**context):
    import psycopg2 as pg
    
    sql_query = f"""
        INSERT INTO admin_agg_table_pntra
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
               '{context['ds']}'::timestamp
        FROM admin_table_pntra
        WHERE created_at >= '{context['ds']}'::timestamp
          AND created_at < '{context['ds']}'::timestamp + INTERVAL '1 days'
        GROUP BY lti_user_id, attempt_type;
    """
    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='neondb',
        sslmode='require',
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

def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT *
        FROM admin_agg_table_pntra
        WHERE date >= '{context['ds']}'::timestamp
          AND date < '{context['ds']}'::timestamp + INTERVAL '1 days';
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='neondb',
        sslmode='require',
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
        Key=f"admin_{context['ds']}.csv"
    )

with DAG(
    dag_id='admin_agg_dag_pntra',
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    max_active_runs=1,
    max_active_tasks=1
    
) as dag:
    dag_start = EmptyOperator(
        task_id='dag_start'
    )
    dag_end = EmptyOperator(
        task_id='dag_end'
    )

    combine_data_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )
    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
    )
  
    dag_start >> combine_data_task >> upload_data_task >> dag_end


