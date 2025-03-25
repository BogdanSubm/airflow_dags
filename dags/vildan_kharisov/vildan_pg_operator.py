import psycopg2 as pg
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

class PostgresOperator(BaseOperator):
    def __init__(self,date_from, date_to,**kwargs):
        template_fields = ('date_from', 'date_to')
        super().__init__(**kwargs)
        #self.sql_query = sql_query
        self.date_from = date_from
        self.date_to = date_to

    def execute(self,context):
        # бизнес-логика
        query = f"""
            INSERT INTO vildan_agg_table
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1),
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
                   {self.date_from}::timestamp
              FROM vildan_kharisov_table
             WHERE created_at >= {self.date_from}::timestamp
                   AND created_at < {self.date_to}::timestamp
              GROUP BY lti_user_id, attempt_type;
        """
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
            cursor.execute(query)
            conn.commit()