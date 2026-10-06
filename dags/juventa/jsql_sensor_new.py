from typing import List, Union
import psycopg2 as pg
from contextlib import closing
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

class JSqlSensor1(BaseSensorOperator):
    template_fields = ('sql',)

    def __init__(self, sql:  Union[str, List[str], tuple], **kwargs):
        super().__init__(**kwargs)
        self.sql = [sql] if isinstance(sql, str) else list(sql)

    def poke(self, context) -> bool:
        connection = BaseHook.get_connection('conn_pg')

        with closing(pg.connect(
            dbname='etl',
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
            keepalives_idle=600,
            tcp_user_timeout=600
        )) as conn:
            with conn.cursor() as cursor:
                for query in self.sql:
                    self.log.info(query)
                    cursor.execute(query)
                    result = cursor.fetchone()  #нам не нужны все строки
                    if result is None or result[0] == 0:
                        self.log.info("Condition not met (returned 0 or None). Sensor will poke again.")
                        return False
        self.log.info("All queries returned > 0 rows. Sensor succeeded.")
        return True




