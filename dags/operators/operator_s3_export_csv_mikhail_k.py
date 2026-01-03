# dags/operators/operator_s3_export_csv_mikhail_k.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import boto3
import io

class S3ExportCSVOperator(BaseOperator):
    
    template_fields = ("s3_path", "table_name")

    @apply_defaults
    def __init__(self, table_name, s3_path, postgres_conn_id="conn_pg", s3_conn_id="conn_s3", **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.s3_path = s3_path
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        # Парсим путь
        bucket, key = self.s3_path.replace("s3://", "").split("/", 1)

        #ВАЖНО: берём ds из контекста!
        ds = context["ds"]

        # Фильтрация по ds — только если колонка называется ds
        where_clause = f" WHERE ds = '{ds}'::DATE" if "ds" in self.table_name.lower() else ""
        sql = f"COPY (SELECT * FROM {self.table_name}{where_clause}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"

        self.log.info(f"Выполняем COPY: {sql}")

        # Читаем данные
        buffer = io.StringIO()
        cur = PostgresHook(self.postgres_conn_id).get_conn().cursor()
        cur.copy_expert(sql, buffer)
        csv_data = buffer.getvalue().encode("utf-8")

        if not csv_data.strip():
            self.log.warning(f"Данные пустые для {self.table_name} за {ds} — ничего не экспортируем")
            return

        # РУЧНОЙ boto3 клиент
        conn = BaseHook.get_connection(self.s3_conn_id)
        extra = conn.extra_dejson if conn.extra_dejson else {}
        endpoint_url = extra.get("endpoint_url") or conn.host or ""

        session = boto3.session.Session()
        s3_client = session.client(
            's3',
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            endpoint_url=endpoint_url.rstrip("/") if endpoint_url else None
        )

        self.log.info(f"Экспорт {self.table_name} ({len(csv_data)} байт) → s3://{bucket}/{key}")
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_data)
        self.log.info("Экспорт завершён успешно")