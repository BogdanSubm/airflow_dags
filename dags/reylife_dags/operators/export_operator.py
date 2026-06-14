import psycopg2
import pandas as pd
import boto3

from io import StringIO
from typing import Any

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context


class ExportCsvOperator(BaseOperator):

    def __init__(
        self,
        pg_conn_id: str = "conn_pg",
        s3_conn_id: str = "conn_s3",
        bucket: str = "reylife-bucket",
        key: str = "result.csv",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.pg_conn_id = pg_conn_id
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket
        self.key = key

    def execute(self, context: Context) -> Any:
        self.log.info("читаем агрегат из postgres...")

        pg = BaseHook.get_connection(self.pg_conn_id)
        db = psycopg2.connect(
            host=pg.host,
            dbname=pg.schema,
            user=pg.login,
            password=pg.password,
            port=pg.port
        )

        df = pd.read_sql("SELECT * FROM reylife_agg", db)
        db.close()

        # пишем датафрейм в буфер чтобы не создавать временный файл на диске
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3 = BaseHook.get_connection(self.s3_conn_id)
        client = boto3.client(
            "s3",
            endpoint_url=s3.host,
            aws_access_key_id=s3.login,
            aws_secret_access_key=s3.password
        )

        client.put_object(
            Bucket=self.bucket,
            Key=self.key,
            Body=csv_buffer.getvalue()
        )

        self.log.info(f"файл загружен в s3://{self.bucket}/{self.key}")
        return f"s3://{self.bucket}/{self.key}"