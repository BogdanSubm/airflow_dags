from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
import psycopg2
from typing import List


class MultiSQLSensor(BaseSensorOperator):
    template_fields = ("sql_checks",)
    def __init__(self, conn_id: str, sql_checks: List[str], **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql_checks = sql_checks
    def poke(self, context) -> bool:
        connection = BaseHook.get_connection(self.conn_id)
        with psycopg2.connect(
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
            with conn.cursor() as cursor:
                for sql in self.sql_checks:
                    self.log.info("Выполняю проверку: %s", sql)
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    if not result or not result[0]:
                        self.log.info("Проверка не прошла: %s", sql)
                        return False
        self.log.info("Все SQL-проверки успешно пройдены")
        return True