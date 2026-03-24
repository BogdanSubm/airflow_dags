from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from roc_hw.plugins import sqls


class CheckTablesSensor(BaseSensorOperator):

    def __init__(self, conn_id, tables_to_check, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.tables_to_check = tables_to_check

    def poke(self, context):

        hook = PostgresHook(self.conn_id)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:

                res = None
                for table in self.tables_to_check:
                    query = sql.SQL(sqls.sql_check_table).format(
                        sql.Identifier(table)).as_string(cur)
                    cur.execute(query)
                    res = cur.fetchone()

                    if not res:
                        return False

                return True
