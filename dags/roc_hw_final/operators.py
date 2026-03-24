from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from roc_hw_final.utils import put_to_s3


class MySqlOperator(BaseOperator):
    def __init__(self, pg_conn_id, task_parameters, s3_conn_id=None, **kwargs):
        super().__init__(**kwargs)
        self.pg_conn_id = pg_conn_id
        self.s3_conn_id = s3_conn_id
        self.table_name = task_parameters['table_name']
        self.table_ddl = task_parameters['table_ddl']
        self.table_dml = task_parameters['table_dml']
        self.export_flag = task_parameters['need_to_export']

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)

        with pg_hook.get_conn() as pg_conn:

            with pg_conn.cursor() as cur:

                # create if create table if not exists
                query = sql.SQL(self.table_ddl).format(
                    sql.Identifier(self.table_name)).as_string(cur)
                cur.execute(query)
                pg_conn.commit()

                #  truncate to follow idempotence
                query = sql.SQL("TRUNCATE TABLE {}").format(
                    sql.Identifier(self.table_name)).as_string(cur)
                cur.execute(query)
                pg_conn.commit()

                # aggregation
                query = self.table_dml
                cur.execute(query)
                body = cur.fetchall()

                # insert agg_body to table
                pg_hook.insert_rows(
                    table=self.table_name,
                    rows=body,
                    target_fields=['category', 'amount'],
                )

                # export headers + body to s3
                if self.export_flag:

                    # make columns_names list
                    query = """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """
                    cur.execute(query, (self.table_name,))
                    headers = [tup[0] for tup in cur.fetchall()]

                    # export
                    put_to_s3(
                        context=context,
                        s3_conn_id=self.s3_conn_id,
                        headers=headers,
                        body=body
                    )
