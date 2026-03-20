from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql


class TablesResetOperator(BaseOperator):

    def __init__(self, conn_id, tables_to_reset: tuple, sql_creates_dict: dict, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.tables_to_reset = tables_to_reset
        self.sql_creates_dict = sql_creates_dict

    def execute(self, context):

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                for table in self.tables_to_reset:

                    drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(table)).as_string(cur)
                    create_query = self.sql_creates_dict[table]

                    cur.execute(drop_query)
                    cur.execute(create_query)

                    self.log.info(f"=> table {table} reset staged")

        self.log.info("=> tables reset OK")


class PgOperator(BaseOperator):

    template_fields = ('sql_query_parameters',)

    def __init__(self, conn_id, sql_query, sql_query_parameters, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.sql_query_parameters = sql_query_parameters

    def execute(self, context):

        hook = PostgresHook(postgres_conn_id=self.conn_id)

        hook.run(
            sql=self.sql_query,
            parameters=self.sql_query_parameters
        )
