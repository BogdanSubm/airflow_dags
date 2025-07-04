from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

import psycopg2 as pg

class PostgresOperator(BaseOperator):
    template_fields = ('date_from')

    def __init__(self, date_from: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
    
    def execute(self, context):
        connection = BaseHook.get_connection('conn_pg')

        with pg.connect(
            dbname="etl",
            sslmode="disable",
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
            keepalives_idle=600,
            tcp_user_timeout=600
        ) as conn:
            
            cursor = conn.cursor()
            cursor.execute("""SELECT 1 FROM agg_data_ed WHERE date_start = %s""", self.date_from)
            if not cursor.fetchone():
                sql_query = """
                    INSERT INTO agg_data_ed
                        SELECT
                            lti_user_id,
                            attempt_type,
                            %s AS date_start,
                            COUNT(is_correct) AS attempt_count,
                            SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS attempt_count_true,
                            SUM(CASE WHEN is_correct THEN 0 ELSE 1 END) AS attempt_count_false
                        FROM
                            raw_data_ed
                        WHERE
                            TO_CHAR(created_at, 'YYYY-MM-DD') = %s
                        GROUP BY
                            lti_user_id, attempt_type;
                """
            cursor.execute(sql_query, self.date_from, self.date_from)
            conn.commit()
