# Импорт библиотек
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from operators.api_to_pg_operator_ed import APIToPgOperator
from operators.branch_operator_ed import BranchOperator
from operators.pg_operator_ed import PostgresOperator

from datetime import datetime

# Аргументы по умолчанию
DEFAULT_ARGS = {
    "owner": "ed",
    "retries": 2,
    "retry_delay": 600,
    "start_date": datetime(2025, 7, 1),
}

def upload_data(dt: str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = """
        SELECT * FROM raw_data_ed;
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

    year, month, _ = dt.split('-')
   
    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"ed_{year}-{int(month):02d}.csv"
    )

# Параметры DAG
with DAG(
    dag_id='load_api_pg_s3_ed_13',
    tags=['ed', '13'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    raw_data = APIToPgOperator(
        task_id='raw_data',
        date_from='{{ ds }}',
        date_to='{{ ds }}'
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={'dt': '{{ ds }}'}
    )

    branch = BranchOperator(
        task_id='branch',
        dt='{{ ds }}',
        num_days=[1, 2, 5]
    )

    agg_data = PostgresOperator(
        task_id='agg_data',
        date_from='{{ ds }}'
    )

    dag_start >> raw_data >> upload_data >> branch
    branch >> agg_data
    agg_data >> dag_end
    branch >> dag_end