import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator


class PostgresOperator(BaseOperator):

    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
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
            tcp_user_timeout=600,
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(self.sql)
            conn.commit()