from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator


class PostgresOperator(BaseOperator):
    template_fields = ('sql', 'parameters')
    template_ext = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        *,
        sql,
        conn_id='conn_pg',
        dbname='etl',
        parameters=None,
        autocommit=False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.dbname = dbname
        self.parameters = parameters
        self.autocommit = autocommit

    def get_conn(self):
        import psycopg2

        creds = BaseHook.get_connection(self.conn_id)
        return psycopg2.connect(
            dbname=self.dbname,
            sslmode='disable',
            user=creds.login,
            password=creds.password,
            host=creds.host,
            port=creds.port,
            connect_timeout=600,
            tcp_user_timeout=600,
        )

    def execute(self, context):
        statements = [self.sql] if isinstance(self.sql, str) else list(self.sql)

        with self.get_conn() as conn:
            conn.autocommit = self.autocommit
            cursor = conn.cursor()
            for statement in statements:
                if not statement or not statement.strip():
                    continue
                self.log.info('Executing SQL:\n%s', statement)
                cursor.execute(statement, self.parameters)
            if not self.autocommit:
                conn.commit()
