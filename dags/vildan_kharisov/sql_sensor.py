import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator


class SqlSensor(BaseSensorOperator):
    template_fields = ('sql', )

    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def poke(self, context) -> bool:
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
            flag = False
            for i in self.sql.values():
                self.log.info(i)
                cursor.execute(i)
                result = cursor.fetchone()
                if result[0]>0:
                    flag = True
                else:
                    flag = False
        return flag


            # self.log.info(self.sql['query_1'])
            #
            # cursor.execute(self.sql['query_1'])
            # result = cursor.fetchone()

        # if result[0] > 0:
        #     return True
        # else:
        #     return False