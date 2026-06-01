from airflow.models import BaseOperator
from OP.f0rtunaa_pgcon import _get_pg_conn


class AggTableOperator(BaseOperator):
    """
    1. Создаёт таблицу если не существует (DDL)
    2. Выполняет DML (INSERT ... SELECT)
    """

    template_fields = ('table_dml',)

    def __init__(self, table_name, table_ddl, table_dml, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.table_ddl = table_ddl
        self.table_dml = table_dml

    def execute(self, context):
        ddl = self.table_ddl.format(table_name=self.table_name)
        dml = self.table_dml.format(table_name=self.table_name)
        with _get_pg_conn() as conn:
            cursor = conn.cursor()

            self.log.info('Создаю таблицу %s', self.table_name)
            cursor.execute(ddl)

            self.log.info('Удаляю старые данные за %s', context['ds'])
            cursor.execute(
                f'DELETE FROM {self.table_name} WHERE load_date = %s',
                (context['ds'],),
            )
            self.log.info('Наполняю таблицу %s', self.table_name)
            cursor.execute(dml)
            self.log.info('Вставлено строк: %s', cursor.rowcount)

            conn.commit()