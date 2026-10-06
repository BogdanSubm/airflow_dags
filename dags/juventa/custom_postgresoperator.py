import psycopg2 as pg

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from typing import Any, Optional, Sequence


class JCustomPostgresOperator(BaseOperator):
    # Для jinja объявляем template_fields
    template_fields = ('sql_query', 'parameters')

    #Инициализация
    def __init__(
            self,
            sql_query: str,
            dbname: str,
            postgres_conn_id: str,
            parameters: Optional[Sequence[Any]] = None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.sql_query = sql_query
        self.dbname = dbname
        self.parameters = parameters or ()
        self.postgres_conn_id = postgres_conn_id

    @staticmethod
    def is_select_query(sql_query: str) -> bool:
        """Проверяет, является ли SQL-запрос SELECT-запросом."""
        if not sql_query:
            return False

        # Убираем пробелы, переводы строк и приводим к нижнему регистру
        normalized_query = sql_query.strip().lower()

        # Проверяем, начинается ли запрос с SELECT или WITH (CTE)
        return normalized_query.startswith('select') or normalized_query.startswith('with')


    # То что будет выполняться при вызове оператора
    def execute(self, context):
        if self.is_select_query(self.sql_query):
            raise ValueError(
                "Этот оператор предназначен только для DML/DDL операций "
                "(INSERT, UPDATE, DELETE, TRUNCATE). Для SELECT используйте другой оператор."
            )

        self.log.info("Executing query on DB: %s", self.dbname)
        # Соединение
        connection = BaseHook.get_connection(self.postgres_conn_id)
        with pg.connect(
                dbname=self.dbname,
                #sslmode='disable',
                user=connection.login,
                password=connection.password,
                host=connection.host,
                port=connection.port,
                connect_timeout=600,
                keepalives_idle=600,
                tcp_user_timeout=600
        ) as conn:

            with conn.cursor() as cursor:
                rendered_query = cursor.mogrify(self.sql_query, self.parameters).decode('utf-8')
                self.log.info("Executing SQL query:\n%s", rendered_query)

                cursor.execute(self.sql_query, self.parameters)
                self.log.info("Statement completed; rowcount=%s", cursor.rowcount)

                conn.commit()



