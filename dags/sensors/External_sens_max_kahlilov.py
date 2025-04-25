import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator

class MultiTableSqlSensor(BaseSensorOperator):
    """
    Сенсор для проверки наличия данных в нескольких таблицах.
    Возвращает True только если все указанные таблицы содержат данные.
    """
    # Добавляем sql_template в template_fields для обработки Jinja макросов
    template_fields = ('tables', 'sql_template')

    def __init__(self, tables: list, date_filter: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.tables = tables
        self.date_filter = date_filter
        # Определяем шаблон SQL запроса как атрибут класса
        self.sql_template = """
            SELECT COUNT(1)
            FROM {table}
            WHERE created_at >= '{{ ds }}'::timestamp
            AND created_at < '{{ ds }}'::timestamp + INTERVAL '1 days'
        """ if date_filter else "SELECT COUNT(1) FROM {table}"

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
                # Форматируем SQL запрос, подставляя имя таблицы
                sql = self.sql_template.format(table=table)
                
                self.log.info(f"Проверка таблицы {table} с запросом: {sql}")
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result[0] <= 0:
                    self.log.info(f"Таблица {table} не содержит данных")
                    return False
                
                self.log.info(f"Таблица {table} содержит {result[0]} записей")
            
            return True