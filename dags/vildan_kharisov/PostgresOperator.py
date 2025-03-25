import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class PostgresOperator(BaseOperator):
    def __init__(self,sql_query,*args,**kwargs):
        template_fields = ('sql_query',)
        super().__init__(*args,**kwargs)
        self.sql_query = sql_query

    def execute(self,sql_query,context):
        # бизнес-логика
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
            conn.commit()