import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator


class SQLSensor(BaseSensorOperator):
    template_fields = ('tables',)

    def __init__(self, tables: list, **kwargs):
        super().__init__(**kwargs)
        self.tables = tables

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
            tcp_user_timeout=600,
        ) as conn:
            cursor = conn.cursor()

            for table in self.tables:
                sql = f"SELECT COUNT(1) FROM {table};"

                self.log.info(sql)

                cursor.execute(sql)
                result = cursor.fetchone()

                if result[0] > 0:
                    self.log.info("Table '%s' has data, OK", table)
                else:
                    self.log.info("Table '%s' has no data yet, rescheduling...", table)
                    return False

        return True