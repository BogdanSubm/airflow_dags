# dags/operators/operator_s3_export_csv_mikhail_k.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
from io import StringIO
import logging

class S3ExportCSVOperator(BaseOperator):
    template_fields = ("s3_path", "table_name")

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        s3_path: str,
        postgres_conn_id: str = "conn_pg",
        s3_conn_id: str = "conn_s3",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.s3_path = s3_path
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        ds = context["ds"]
        bucket, key = self.s3_path.replace("s3://", "").split("/", 1)

        # === 1. Правильный SQL с фильтрацией по ds ===
        where_clause = f" WHERE ds = '{ds}'::DATE" if "ds" in self.table_name.lower() else ""
        sql = f"""
            COPY (
                SELECT * FROM {self.table_name}{where_clause}
            ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
        """

        logging.info(f"Экспорт таблицы {self.table_name} за {ds}")
        logging.info(f"SQL: {sql.strip()}")

        # === 2. Выполняем COPY через psycopg2 (самый надёжный способ) ===
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        buffer = StringIO()
        cursor.copy_expert(sql, buffer)
        buffer.seek(0)
        csv_content = buffer.getvalue()

        if not csv_content.strip():
            logging.warning(f"Нет данных для экспорта: {self.table_name} за {ds}")
            return

        # === 3. Загружаем в MinIO/S3 — в твоём стиле ===
        s3_conn = BaseHook.get_connection(self.s3_conn_id)
        endpoint_url = s3_conn.host or s3_conn.extra_dejson.get("endpoint_url")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url.rstrip("/") if endpoint_url else None,
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            config=Config(signature_version="s3v4"),
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv"
        )

        logging.info(f"УСПЕШНО экспортировано → s3://{bucket}/{key} ({len(csv_content)} байт)")