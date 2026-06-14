from datetime import datetime
from io import StringIO

import psycopg2
import pandas as pd
import boto3

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context

# я понимаю что по-хорошему это надо вынесли в другую папку, но у меня не работают
# правильно импорты, я пыталась импортить операторы со своей папки
# from reylife_dags.operators... но он пишет что не может найти папку dags(хотя я ее не упоминаю
# и понимаю что она корневая и ее писать не надо
# ну короче решила просто здесь оставить чтобы работало хотя бы
class CustomPostgresOperator(BaseOperator):
    """
    просто выполняет sql запросы к postgres, ничего не возвращает
    """

    template_fields = ('sql',)

    def __init__(
        self,
        sql: str | list[str],
        pg_conn_id: str = "conn_pg",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.pg_conn_id = pg_conn_id

    def execute(self, context: Context):

        conn = BaseHook.get_connection(self.pg_conn_id)
        db = psycopg2.connect(
            host=conn.host,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        )
        cur = db.cursor()

        # если один запрос то оборачиваем в список чтобы не писать два раза одно и то же
        queries = self.sql if isinstance(self.sql, list) else [self.sql]

        for query in queries:
            self.log.info(f"выполняем запрос:\n{query}")
            cur.execute(query)

        db.commit()
        db.close()
        self.log.info("готово")


class ExportCsvOperator(BaseOperator):
    """
    читает таблицу из postgres и кладёт csv в s3
    """

    def __init__(
        self,
        query: str,
        key: str,
        pg_conn_id: str = "conn_pg",
        s3_conn_id: str = "conn_s3",
        bucket: str = "reylife-bucket",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.query = query
        self.key = key
        self.pg_conn_id = pg_conn_id
        self.s3_conn_id = s3_conn_id
        self.bucket = bucket

    def execute(self, context: Context):
        self.log.info(f"читаем данные запросом: {self.query}")

        pg = BaseHook.get_connection(self.pg_conn_id)
        db = psycopg2.connect(
            host=pg.host,
            dbname=pg.schema,
            user=pg.login,
            password=pg.password,
            port=pg.port
        )

        df = pd.read_sql(self.query, db)
        db.close()

        # пишем в буфер чтобы не создавать файл на диске
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

# конфиг по-хорошему наверное тоже надо вынести...
config = [
    {
        'table_name': 'reylife_agg_by_user',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS reylife_agg_by_user (
                lti_user_id TEXT,
                total_attempts INT,
                correct_answers INT
            )
        """,
        'table_dml': """
            DELETE FROM reylife_agg_by_user;
            INSERT INTO reylife_agg_by_user
            SELECT
                lti_user_id,
                COUNT(*) AS total_attempts,
                COUNT(*) FILTER (WHERE is_correct = true) AS correct_answers
            FROM reylife_raw
            GROUP BY lti_user_id
        """,
        'need_to_export': True
    },
    {
        'table_name': 'reylife_agg_by_day',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS reylife_agg_by_day (
                day DATE,
                total_attempts INT
            )
        """,
        'table_dml': """
            DELETE FROM reylife_agg_by_day;
            INSERT INTO reylife_agg_by_day
            SELECT
                created_at::date AS day,
                COUNT(*) AS total_attempts
            FROM reylife_raw
            GROUP BY created_at::date
        """,
        'need_to_export': False
    }
]

DEFAULT_ARGS = {
    "owner": "reylife",
    "start_date": datetime(2026, 6, 10),
    "retries": 2
}

with DAG(
    dag_id="reylife_dynamic_dag",
    schedule="@daily",
    tags=["reylife", "@TvoiRaiii"],
    default_args=DEFAULT_ARGS,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for table in config:

        create = CustomPostgresOperator(
            task_id=f"create_{table['table_name']}",
            sql=table['table_ddl']
        )

        insert = CustomPostgresOperator(
            task_id=f"insert_{table['table_name']}",
            # delete + insert идемпотентностьпотому что каждый раз пересчитываем с нуля
            sql=table['table_dml']
        )

        if table['need_to_export']:
            export = ExportCsvOperator(
                task_id=f"export_{table['table_name']}",
                query=f"SELECT * FROM {table['table_name']}",
                key=f"{table['table_name']}.csv"
            )
            start >> create >> insert >> export >> end
        else:
            start >> create >> insert >> end