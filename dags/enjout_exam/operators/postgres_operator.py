import re
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

ALLOWED_KEYWORDS = frozenset({
    'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
    'TRUNCATE', 'GRANT', 'REVOKE', 'COMMENT', 'VACUUM', 'ANALYZE',
    'COPY', 'CALL', 'DO', 'WITH',
})


class PostgresOperator(BaseOperator):
    """Выполняет DML/DDL запросы к PostgreSQL. SELECT-запросы запрещены."""

    template_fields: Sequence[str] = ('sql', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#336791'

    @apply_defaults
    def __init__(
        self,
        sql: Union[str, List[str]],
        postgres_conn_id: str = 'conn_pg',
        database: str = 'etl',
        parameters: Optional[Union[Dict[str, Any], List, tuple]] = None,
        autocommit: bool = True,
        sslmode: str = 'disable',
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.parameters = parameters
        self.autocommit = autocommit
        self.sslmode = sslmode

    @staticmethod
    def _first_keyword(sql: str) -> str:
        cleaned = re.sub(r'--[^\n]*', '', sql)
        cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
        cleaned = cleaned.strip().rstrip(';').strip()
        if not cleaned:
            return ''
        return cleaned.split()[0].upper()

    def _validate_sql(self, sql: str) -> None:
        keyword = self._first_keyword(sql)
        if keyword == 'SELECT':
            raise ValueError('PostgresOperator не поддерживает SELECT-запросы.')
        if keyword and keyword not in ALLOWED_KEYWORDS:
            raise ValueError(f'Недопустимый тип SQL-запроса: {keyword}.')

    def _get_statements(self) -> List[str]:
        if isinstance(self.sql, str):
            return [self.sql]
        return list(self.sql)

    def execute(self, context: Any) -> None:
        import psycopg2 as pg

        statements = self._get_statements()
        for statement in statements:
            self._validate_sql(statement)

        connection = BaseHook.get_connection(self.postgres_conn_id)

        with pg.connect(
            dbname=self.database,
            sslmode=self.sslmode,
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
            keepalives_idle=600,
            tcp_user_timeout=600,
        ) as conn:
            with conn.cursor() as cursor:
                for index, statement in enumerate(statements, start=1):
                    preview = ' '.join(statement.split())[:200]
                    self.log.info('SQL [%s/%s]: %s...', index, len(statements), preview)
                    cursor.execute(statement, self.parameters)
                    if cursor.rowcount >= 0:
                        self.log.info('Затронуто строк: %s', cursor.rowcount)

            if self.autocommit:
                conn.commit()
