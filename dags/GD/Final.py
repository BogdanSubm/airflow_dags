from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
from jinja2 import Template
import psycopg2 as pg
from io import BytesIO
import csv
import boto3 as s3
from botocore.client import Config
import codecs

log = logging.getLogger(__name__)

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PG_CONN_ID = "conn_pg"
S3_CONN_ID = "conn_s3"


def execute_ddl(table_name, table_ddl, conn_id=PG_CONN_ID):
    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            log.info(f"Executing DDL for {table_name}")
            cur.execute(table_ddl)
        conn.commit()


def execute_dml(table_name, dml_query, conn_id=PG_CONN_ID, **context):

 
    rendered_query = Template(dml_query).render(**context)

    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            log.info(f"Truncating {table_name} before load")
            cur.execute(f"TRUNCATE TABLE {table_name}")

            insert_query = f"INSERT INTO {table_name} SELECT * FROM ({rendered_query}) AS src"
            log.info(f"Inserting data into {table_name}")
            cur.execute(insert_query)
        conn.commit()

    # Контроль качества: таблица не должна быть пустой
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"Table {table_name} is empty after load. Data quality issue.")
    log.info(f"Data load completed for {table_name}. Rows: {count}")


def upload_data(**context):
    
    sql_query = """
        SELECT lti_user_id, total_rows
        FROM public.test_gd_agg;
    """

    connection = BaseHook.get_connection(PG_CONN_ID)

    with pg.connect(
        dbname=connection.schema or 'etl',  # fallback на 'etl' если schema не задан
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port or 5432,
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

    s3_conn = BaseHook.get_connection(S3_CONN_ID)

    s3_client = s3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version="s3v4"),
    )

    ds = context['ds']  # формат YYYY-MM-DD
    bucket_name = 'gdi'
    key = f"gd_{ds}.csv"

    log.info(f"Uploading to s3://{bucket_name}/{key}")
    s3_client.put_object(
        Body=file,
        Bucket=bucket_name,
        Key=key
    )



config = [
    {
       
        "table_name": "test_gd_agg",
        "table_ddl": """
            CREATE TABLE IF NOT EXISTS public.test_gd_agg (
                lti_user_id varchar(1000),
                total_rows bigint
            )
        """,
       
        "table_dml": """
            SELECT
                lti_user_id,
                COUNT(*) AS total_rows
            FROM public.test_gd
            GROUP BY lti_user_id
        """,
        "need_to_export": True,
    },
]

with DAG(
    dag_id="dynamic_airflow_load_test_gd_custom_upload",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airflow", "dynamic", "etl", "test_gd", "custom_s3"],
) as dag:

    for cfg in config:
        table_name = cfg["table_name"]
        ddl = cfg["table_ddl"]
        dml = cfg["table_dml"]
        need_export = cfg.get("need_to_export", False)

        with TaskGroup(group_id=f"tg_{table_name}", tooltip=f"Tasks for {table_name}") as tg:

            create_task = PythonOperator(
                task_id=f"create_{table_name}",
                python_callable=execute_ddl,
                op_kwargs={"table_name": table_name, "table_ddl": ddl},
            )

            load_task = PythonOperator(
                task_id=f"load_{table_name}",
                python_callable=execute_dml,
                op_kwargs={"table_name": table_name, "dml_query": dml},
            )

            create_task >> load_task

            if need_export:
                export_task = PythonOperator(
                    task_id=f"export_{table_name}",
                    python_callable=upload_data,
                    provide_context=True, 
                )
                load_task >> export_task