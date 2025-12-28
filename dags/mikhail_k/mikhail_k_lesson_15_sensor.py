import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

class SqlSensor(BaseSensorOperator):
    template_fields = ('sql', ) # данный параметр прописывается, чтобы можно было использовать переменные в sql запросе, как template jinja

    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def poke(self, context) -> bool: # данный метод проверяет, выполнился ли запрос. Он идет вместо execute, который не обязательно что-то возвращает.
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
            cursor = conn.cursor() # Создаем курсор 
            self.log.info(self.sql) # Логирование sql запроса
            cursor.execute(self.sql) # Выполняем запрос
            result = cursor.fetchone() # Получаем данные. fetchone - получает одну строку

        if result[0] > 0:
            return True
        else:
            return False
            
