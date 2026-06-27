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

from GD.Final.config import config

log = logging.getLogger(__name__)

default_args = {
    "owner": "GDI",
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

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
    
    if count == 0:
        raise ValueError(f"Table {table_name} is empty after load.")
    
    log.info(f"Data load completed for {table_name}. Rows: {count}")


def upload_data(table_name, columns, conn_id=PG_CONN_ID, **context):
    """
    Выгружает данные в S3.
    1. Формирует SELECT.
    2. Читает данные из PG.
    3. Удаляет старый файл в S3 (если есть).
    4. Загружает новый.
    """
    # 1. Формируем SELECT
    if columns == ["*"]:
        select_clause = "*"
    else:
        select_clause = ", ".join(f'"{col}"' for col in columns)

    sql_query = f'SELECT {select_clause} FROM public."{table_name}"'

    # 2. Читаем данные из PostgreSQL
    connection = BaseHook.get_connection(conn_id)

    with pg.connect(
        dbname=connection.schema or 'etl',
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
        log.info(f"Running query for export: {sql_query}")
        cursor.execute(sql_query)
        data = cursor.fetchall()

    # Готовим CSV в памяти
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

    # 3. Подключаемся к S3
    s3_conn = BaseHook.get_connection(S3_CONN_ID)
    s3_client = s3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version="s3v4"),
    )

    ds = context['ds']  # YYYY-MM-DD
    bucket_name = 'gdi'
    key = f"{table_name}/{ds}.csv"

    # 4. УДАЛЯЕМ СТАРЫЙ ФАЙЛ
    log.info(f"Checking and deleting old object: s3://{bucket_name}/{key}")
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        log.info("Old S3 object deleted.")
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            # Это нормально: файла не было
            log.info("No old object found to delete.")
        else:
            # Любая другая ошибка — это проблема
            log.error(f"Error while deleting S3 object: {e}")
            raise

    # 5. Загружаем новый файл
    log.info(f"Uploading new object: s3://{bucket_name}/{key}")
    s3_client.put_object(
        Body=file,
        Bucket=bucket_name,
        Key=key
    )
    log.info("Upload completed successfully.")


with DAG(
    dag_id="dynamic_airflow_from_config",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airflow", "dynamic", "etl", "s3-clean"],
) as dag:

    for cfg in config:
        table_name = cfg["table_name"]
        ddl = cfg["table_ddl"]
        dml = cfg["table_dml"]
        need_export = cfg.get("need_to_export", False)
        export_columns = cfg.get("export_columns", ["*"])

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
                    op_kwargs={
                        "table_name": table_name,
                        "columns": export_columns,
                    },
                )
                load_task >> export_task
