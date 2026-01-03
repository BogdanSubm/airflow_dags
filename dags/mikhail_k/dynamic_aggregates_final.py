# dags/mikhail_k/dynamic_aggregates_final.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from jinja2 import Template
import boto3
import logging

def run_all_aggregates(**context):
    """
    Один оператор — всё делает:
    - Читает конфиг из S3
    - Для каждого агрегата: создаёт таблицу, проверяет пустоту, грузит данные, экспортирует
    - Полностью идемпотентный
    """
    ds = context["ds"]
    logging.info(f"Запуск агрегаций за {ds}")

    # === 1. Читаем конфиг из MinIO через conn_s3 (даже если оно FTP) ===
    conn = BaseHook.get_connection("conn_s3")
    endpoint = conn.host or conn.extra_dejson.get("endpoint_url", "")
    if endpoint.startswith("http://"):
        endpoint = endpoint
    elif endpoint:
        endpoint = f"http://{endpoint}"

    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=endpoint.rstrip("/")
    )

    obj = s3.get_object(Bucket="mikhail-k", Key="agg_config.conf")
    content = obj["Body"].read().decode("utf-8")

    # === 2. Выполняем конфиг как Python-код ===
    namespace = {}
    exec(content, namespace)
    config = namespace.get("config")
    if not config:
        raise ValueError("В agg_config.conf нет переменной 'config'")

    # === 3. Обрабатываем каждый агрегат ===
    pg_hook = PostgresHook("conn_pg")

    for agg in config:
        table_name = agg["table_name"]
        logging.info(f"Обработка таблицы: {table_name}")

        # 1. Создаём таблицу (идемпотентно)
        ddl = Template(agg["table_ddl"]).render(table_name=table_name)
        pg_hook.run(ddl)

        # 2. Проверяем — есть ли уже данные за ds?
        has_data = pg_hook.get_first(
            f"SELECT 1 FROM {table_name} WHERE ds = '{ds}'::DATE LIMIT 1"
        )
        if has_data:
            logging.info(f"Данные за {ds} уже есть в {table_name} — пропускаем")
            continue

        # 3. Загружаем данные
        dml = Template(agg["table_dml"]).render(table_name=table_name, ds=ds)
        logging.info(f"Загружаем данные в {table_name}")
        pg_hook.run(dml)

        # 4. Экспорт в S3 (если нужно)
        if agg.get("need_to_export"):
            export_path = Template(agg["export_path"]).render(
                table_name=table_name.replace(".", "/"),
                ds=ds
            )
            bucket, key = export_path.replace("s3://", "").split("/", 1)

            # COPY → STDOUT → bytes
            import io
            buffer = io.StringIO()
            pg_hook.get_conn().cursor().copy_expert(
                f"COPY (SELECT * FROM {table_name} WHERE ds = '{ds}'::DATE) TO STDOUT WITH CSV HEADER",
                buffer
            )
            csv_data = buffer.getvalue().encode("utf-8")

            s3.put_object(Bucket=bucket, Key=key, Body=csv_data)
            logging.info(f"Экспортировано → s3://{bucket}/{key}")

    logging.info("Все агрегаты обработаны успешно")

# === DAG ===
with DAG(
    dag_id="dynamic_aggregates_final",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mikhail_k",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aggregates", "simple", "reliable", "mikhail_k"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run = PythonOperator(
        task_id="run_all_aggregates",
        python_callable=run_all_aggregates,
        provide_context=True,
    )

    start >> run >> end