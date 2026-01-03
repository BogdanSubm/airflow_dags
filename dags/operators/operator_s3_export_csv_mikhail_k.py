# dags/operators/operator_s3_export_csv_mikhail_k.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
        bucket, key = self.s3_path.replace("s3://", "").split("/", 1)
        where = " WHERE ds = '{{ ds }}'::DATE" if "ds" in self.table_name.lower() else ""

        sql = f"COPY (SELECT * FROM {self.table_name}{where}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"

        pg_hook = PostgresHook(self.postgres_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        buffer = io.StringIO()
        cur = pg_hook.get_conn().cursor()
        cur.copy_expert(sql, buffer)

        s3_hook.load_bytes(
            bytes_data=buffer.getvalue().encode("utf-8"),
            key=key,
            bucket_name=bucket,
            replace=True
        )
        self.log.info(f"Экспортировано → s3://{bucket}/{key}")