import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

class MultiTableSqlSensor(BaseSensorOperator):
    """
    Сенсор для проверки наличия данных в нескольких таблицах.
    Возвращает True только если все указанные таблицы содержат данные.
    """
    template_fields = ('tables', 'sql')  # Добавляем 'sql' в template_fields

    def __init__(self, tables: list, date_filter: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.tables = tables
        self.date_filter = date_filter
        self.sql = None  # Добавляем атрибут для хранения SQL-запроса

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

            for table in self.tables:
                if self.date_filter:
                    self.sql = f"""
                        SELECT COUNT(1)
                        FROM {table}
                        WHERE created_at >= '{{ ds }}'::timestamp
                        AND created_at < '{{ ds }}'::timestamp + INTERVAL '1 days'
                    """
                else:
                    self.sql = f"SELECT COUNT(1) FROM {table}"
                
                self.log.info(f"Проверка таблицы {table} с запросом: {self.sql}")
                cursor.execute(self.sql)
                result = cursor.fetchone()
                
                if result[0] <= 0:
                    self.log.info(f"Таблица {table} не содержит данных")
                    return False
                
                self.log.info(f"Таблица {table} содержит {result[0]} записей")
            
            return True