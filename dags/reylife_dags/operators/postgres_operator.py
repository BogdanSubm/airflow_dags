import psycopg2
from typing import Any

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context


class CustomPostgresOperator(BaseOperator):
    """
    универсальный оператор для запросов к postgres
    select не используем птмчт оператор ничего не возвращает
    """

    # template_fields нужен чтобы джинджа могла рендерить {{ ds }} прямо в sql
    template_fields = ('sql',)
    template_ext = ('.sql',)

    def __init__(
        self,
        sql: str | list[str],
        pg_conn_id: str = "conn_pg",
        autocommit: bool = True,
        parameters: dict | list | None = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.pg_conn_id = pg_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context: Context) -> Any:

        conn = BaseHook.get_connection(self.pg_conn_id)
        db = psycopg2.connect(
            host=conn.host,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        )
        cur = db.cursor()

        # если передали один запрос то оборачиваем в список чтобы не писать два раза одно и то же
        queries = self.sql if isinstance(self.sql, list) else [self.sql]

        for query in queries:
            self.log.info(f"выполняем запрос:\n{query}")
            cur.execute(query, self.parameters)

        if self.autocommit:
            db.commit()

        db.close()
        self.log.info("готово")