from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

import psycopg2 as pg


class MultiTableSQLSensor(BaseSensorOperator):
    """
    Параметры:
        checks  — список словарей с проверками, каждый содержит:
                    - table : имя таблицы
                    - sql   : SELECT-запрос, который должен вернуть хотя бы одну строку
        conn_id — ID Airflow-подключения (по умолчанию 'conn_pg')
        dbname  — имя БД (по умолчанию 'etl')
    """

    template_fields = ('checks',)

    def __init__(self, checks, conn_id='conn_pg', dbname='etl', **kwargs):
        kwargs.setdefault('mode', 'reschedule')
        super().__init__(**kwargs)
        self.checks = checks
        self.conn_id = conn_id
        self.dbname = dbname

    def poke(self, context):
        connection = BaseHook.get_connection(self.conn_id)
        with pg.connect(
            dbname=self.dbname,
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
            all_ready = True

            for check in self.checks:
                table = check['table']
                sql   = check['sql']

                cursor.execute(sql)
                result = cursor.fetchone()

                if result:
                    self.log.info('✔ Таблица "%s" — данные есть', table)
                else:
                    self.log.info('✘ Таблица "%s" — данных нет, ждём...', table)
                    all_ready = False

        return all_ready