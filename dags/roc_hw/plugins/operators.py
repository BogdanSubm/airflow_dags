from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from roc_hw.plugins import sqls


class TablesResetOperator(BaseOperator):

    def __init__(self, conn_id, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        option = context['params'].get('delete/reset')
        sql_creates_dict = sqls.sql_creates_dict

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                for table, query in sql_creates_dict.items():

                    drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table)).as_string(cur)
                    cur.execute(drop_query)

                    if option == 'reset':
                        create_query = sql.SQL(query).format(
                            sql.Identifier(table)).as_string(cur)
                        cur.execute(create_query)


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
