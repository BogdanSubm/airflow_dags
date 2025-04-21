from typing import Any

from airflow.models import BaseOperator

from airflow.hooks.base import BaseHook

import psycopg2 as pg

class CustomCombineDataOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: Any):
        sql_query = f"""
            TRUNCATE TABLE maks_khalilov_agr;
            INSERT INTO maks_khalilov_agr
            SELECT lti_user_id,
                attempt_type,
                COUNT(1),
                COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_fails_count,
                '{context['ds']}'::timestamp
            FROM maks_khalilov
            WHERE created_at >= '{context['ds']}'::timestamp
                AND created_at < '{context['ds']}'::timestamp + INTERVAL '1 days'
            GROUP BY lti_user_id, attempt_type;
        """

        connection = BaseHook.get_connection('conn_pg') # Получаем соединение с базой данных (эти данные находятся в Airflow во вкладке Connections)
        # Создаем соединение с базой данных
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
            cursor.execute(sql_query) # Выполняем запрос
            conn.commit() # Сохраняем изменения