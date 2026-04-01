import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

class MultiSqlSensor(BaseSensorOperator):
    template_fields = ('sql_list',)

    def __init__(self, sql_list, *args,**kwargs):
        super().__init__(*args,**kwargs)
        self.sql_list = sql_list

    def poke(self, context):
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

            for i, sql in enumerate(self.sql_list):
                cursor.execute(sql)
                result = cursor.fetchone()

                if not result or result[0] == 0:
                    self.log.info(f'Query {i+1} returned no data')
                    return False
        return True
