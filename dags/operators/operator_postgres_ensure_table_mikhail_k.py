# dags/operators/operator_postgres_ensure_table_mikhail_k.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template

class PostgresEnsureTableOperator(BaseOperator):
    template_fields = ("table_name", "ddl_template")

    @apply_defaults
    def __init__(self, table_name, ddl_template, postgres_conn_id="conn_pg", **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.ddl_template = ddl_template
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        ddl = Template(self.ddl_template).render(table_name=self.table_name)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(ddl)
        self.log.info(f"Таблица {self.table_name} обеспечена")